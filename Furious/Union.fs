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
       (name.[0..1]) + name.Length.ToString()

    let tryFindTable alias name unions =
        match Map.tryFind name unions with
        | Some t -> fst t
        | None -> { name = name; alias = alias }

    let rec buildUnionPath (unions: Map<string, vertex>) prevVertex prevKey = function
    | (tp,name,keyName)::[] -> 
        match prevVertex with
        | Some (lastTable,_) -> lastTable.alias, unions
        | None -> failwith "union path shorter than expected"
    | (tp,name,keyName)::t -> 
        match prevVertex with
        | Some vertex -> 
            let newTable = (tryFindTable (generateAlias tp unions) tp unions), []
            let prevTable = connect vertex newTable prevKey name
            let newUnions = update ((fst prevTable).name) prevTable unions
            buildUnionPath newUnions (Some newTable) (match keyName with | Some k -> k | None -> failwith "A key name is required") t
        | None ->
            let newTable = tryFindTable (generateAlias tp unions) tp unions
            buildUnionPath unions (Some (newTable, [])) (match keyName with | Some k -> k | None -> failwith "A key name is required")  t
    | [] -> failwith "no union path to compute"

    let rec last = function
    | h::[] -> h
    | h::t -> last t
    | _ -> failwith "empty list"

    let computeExpression expr unions mapper = 
        let path = getPropertyPath mapper expr
        if List.length path <= 2 then 
            unions, (match List.head path with | (tp,_,_) -> tp),
                ( match path with | (f,_,_)::(_,s,_)::_ -> f + "." + s | _ -> failwith "malformed property path" )
        else
            let alias, newUnions = buildUnionPath unions None "" path
            newUnions, (match List.head path with | (tp,_,_) -> tp), alias + "." + (match last path with | _,v,_ -> v)

    let rec getValue unions mapper = function
    | Value (v, tp) -> unions, "", v.ToString()
    | _ as expr -> computeExpression expr unions mapper

    let rec computeFromClauseForSegment previous graph = 
        sprintf "%s %s"
            (match previous with 
             | "" -> sprintf "%s %s" (fst graph).table (fst graph).alias 
             | _ -> previous)
            (sprintf "inner join %s %s on %s.%s = %s.%s"
                

    let computeFromClause unions rootTables =
        let segments = computeSegments <| condenseUnionMap Map.empty unions 
        List.fold (fun state segment ->
                        state + ", " + 
                            match segment.unions with
                            | [] -> sprintf "%s %s" segment.table segment.alias
                            | _ -> computeFromClauseForSegment Map.empty [] segment.unions
                ) "" segments