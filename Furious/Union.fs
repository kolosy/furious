namespace Furious

module Union =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Interfaces
    open TableGraph

    let update key value map = 
        let pruned = 
            match Map.tryFind key map with
            | Some _ -> Map.remove key map
            | None -> map

        Map.add key value pruned

    let rec getPropertyPath (mapper: IRecordMapper) = function
    | PropertyGet (target, prop, idx) -> 
        let remainder = 
            match target with 
            | Some e -> getPropertyPath mapper e
            | None -> failwith "static properties are not supported"
        remainder @ [mapper.MapRecord prop.PropertyType, mapper.MapField prop, mapper.GetPrimaryKeyName prop.PropertyType]
    | Var (e) ->  [ mapper.MapRecord e.Type, "", None ]
    | _ as e -> failwith <| sprintf "%A is an unsupported element" e

    let rec generateAlias (name: string) unions = 
       (name.[0..1]) + name.Length.ToString() + (new System.Random()).Next(100).ToString()

    let tryFindTable alias name unions =
        match Map.tryFind name unions with
        | Some t -> fst t
        | None -> { name = name; alias = alias }

    let tryFindVertex alias name unions =
        match Map.tryFind name unions with
        | Some t -> t
        | None -> ({ name = name; alias = alias }, [])

    let rec buildUnionPath (unions: Map<string, vertex>) prevVertex = function
    | (tp,name,keyName)::[] -> 
        match prevVertex with
        | Some (lastTable,_) -> lastTable.alias, unions
        | None -> failwith "union path shorter than expected"
    | (tp,name,keyName)::t -> 
        match prevVertex with
        | Some vertex -> 
            let newTable = ({ name = tp; alias = generateAlias tp unions }), []
//            let newTable = (tryFindTable (generateAlias tp unions) tp unions), []
            let prevTable = connect vertex newTable name (match keyName with | Some name -> name | None -> failwith "keyname is required")
            let newUnions = update ((fst prevTable).name) prevTable unions
            //printf "%A" newUnions |> ignore
            buildUnionPath newUnions (Some newTable) t
        | None ->
            let newVertex = tryFindVertex (generateAlias tp unions) tp unions
            buildUnionPath unions (Some newVertex) t
    | [] -> failwith "no union path to compute"

    let rec last = function
    | h::[] -> h
    | h::t -> last t
    | _ -> failwith "empty list"

    let computeExpression expr unions mapper = 
        let path = getPropertyPath mapper expr
        if List.length path <= 2 then 
            unions, (match List.head path with | (tp,_,_) -> tp),
                ( match path with 
                  | (f,_,_)::(_,s,_)::_ -> 
                        match Map.tryFind f unions with
                        | Some tbl -> (fst tbl).alias + "." + s 
                        | None -> f + "." + s
                  | _ -> failwith "malformed property path" )
        else
            let alias, newUnions = buildUnionPath unions None path
            newUnions, (match List.head path with | (tp,_,_) -> tp), alias + "." + (match last path with | _,v,_ -> v)

    let rec getValue unions mapper = function
    | Value (v, tp) -> unions, "", v.ToString()
    | _ as expr -> computeExpression expr unions mapper

    let computeUnion (graph: vertex) isBeginning =
        let table, edges = graph

        edges 
        |> ((if isBeginning then (table.name + " as " + table.alias)
                else "") 
        |> List.fold (fun s (t,u) -> sprintf "%s inner join %s as %s on %s.%s = %s.%s" s t.name t.alias table.alias u.sourceColumn t.alias u.targetColumn))

    let rec computeFromClause computedUnions = function
    | h::t -> computeFromClause ((computeUnion h true) :: computedUnions) t
    | [] -> computedUnions