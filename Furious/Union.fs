namespace Furious

module Union =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Interfaces

    type unionMember = {
        target: string
        targetKey: string
        alias: string
    }

    type tableGraph = {
        table: string
        alias: string
        unions: condensedUnionMember list
    } 
    and condensedUnionMember = {
        targetKeyColumn: string
        sourceKeyColumn: string
        targetTable: tableGraph
        sourceTable: tableGraph
    }

    type dbUnion = {
        left: unionMember
        right: unionMember
    }

    let findTable mbr map = match Map.tryFind (mbr.target) map with | Some s -> s | None -> { table = mbr.target; alias = mbr.alias; unions = [] }

    let update key value map = 
        let pruned = 
            match Map.tryFind key map with
            | Some _ -> Map.remove key map
            | None -> map

        Map.add key value pruned

    let rec condenseUnionMap condensed = function
    | (h: dbUnion)::t -> 
        let left = findTable h.left condensed
        let right = findTable h.right condensed
        let union = { sourceKeyColumn = h.left.targetKey; targetKeyColumn = h.right.targetKey; targetTable = left; sourceTable = right }
        condenseUnionMap 
            ((update right.table { right with unions = union :: right.unions } condensed ) |>
             (update left.table { left with unions = union :: left.unions } )) 
            t
    | [] -> condensed

    let rec computeSegments (condensed: Map<string,tableGraph>) = 
        let t = Map.pick (fun key value -> Some value) condensed
        let segment, newCondensed = 
            Map.partition (fun key value -> List.exists (fun elem -> (elem.targetTable = value) || (elem.sourceTable = value)) t.unions) condensed

        (Map.pick (fun key value -> Some value) segment) :: computeSegments newCondensed        

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
       (name.[0..1]) + (List.length unions).ToString()

    let rec buildUnionPath unions (lastMbr: unionMember option) = function
    | (tp,name,keyName)::[] -> 
        match lastMbr with
        | Some mbr -> mbr.alias, unions
        | None -> failwith "union path shorter than expected"
    | (tp,name,keyName)::t -> 
        match lastMbr with
        | Some mbr -> 
            let right = { target = tp; targetKey = tp + "Id"; alias = generateAlias tp unions }
            buildUnionPath ({ left = { mbr with targetKey = name }; right = right } :: unions) (Some right) t
        | None ->
            buildUnionPath unions (Some { target = tp; targetKey = name; alias = generateAlias tp unions }) t
    | [] -> failwith "no union path to compute"

    let rec last = function
    | h::[] -> h
    | h::t -> last t
    | _ -> failwith "empty list"

    let computeExpression expr unions mapper = 
        let path = getPropertyPath mapper expr
        if List.length path <= 2 then 
            unions, 
                ( match path with | (f,_,_)::(_,s,_)::_ -> f + "." + s | _ -> failwith "malformed property path" )
        else
            let alias, newUnions = buildUnionPath unions None path
            newUnions, alias + "." + (match last path with | _,v,_ -> v)

    let rec getValue unions mapper = function
    | Value (v, tp) -> unions, v.ToString()
    | _ as expr -> computeExpression expr unions mapper

    let rec computeFromClauseForSegment includedTables includedRelations = function
    | h::t -> 
        sprintf "%s %s %s"
            (if Map.containsKey h.targetTable includedTables then ""
             else sprintf "%s %s" h.targetTable.table h.targetTable.alias)
            (sprintf " inner join %s %s on %s.%s = %s.%s"
                h.sourceTable.table
                h.sourceTable.alias
                h.targetTable.alias
                h.targetKeyColumn
                h.sourceTable.alias
                h.sourceKeyColumn)
            (computeFromClauseForSegment 
                ((Map.add h.sourceTable h.sourceTable includedTables) |> (Map.add h.sourceTable h.sourceTable)) 
                includedRelations 
                t)
    | [] -> ""

    let computeFromClause unions =
        let segments = computeSegments <| condenseUnionMap Map.empty unions 
        List.fold (fun state segment ->
                        state + ", " + 
                            match segment.unions with
                            | [] -> sprintf "%s %s" segment.table segment.alias
                            | _ -> computeFromClauseForSegment Map.empty [] segment.unions
                ) "" segments