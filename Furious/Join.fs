namespace Furious

module Join =
    open Microsoft.FSharp.Reflection

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Interfaces
    open TypeUtils
    open ValueUtils
    
    type join = {
        table: string
        alias: string
        fkName: string
        pkName: string
        definingType: System.Type
    }

    type compoundJoin =
    | Val of join * compoundJoin list

    let unroll = function
    | Val (j, jl) -> j, jl

    let rec computeJoins (tp: System.Type) (mapper: IRecordMapper) prefix =
        FSharpType.GetRecordFields tp
        |> List.ofArray
        |> List.filter (fun e -> FSharpType.IsRecord e.PropertyType)
        |> List.map (fun e ->
                        Val (
                            { table = mapper.MapRecord e.PropertyType
                              alias = prefix + e.Name
                              fkName = mapper.MapField e
                              pkName = match mapper.GetPrimaryKeyName tp with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" mapper)
                              definingType = e.PropertyType },
                            computeJoins (e.PropertyType) mapper (prefix + e.Name + "_")))

    let rec computeFromClause parentTableName parentTableAlias (mapper: IRecordMapper) isFirst (v: compoundJoin) = 
        let j, jl = unroll v
        sprintf "%s inner join %s as %s on %s.%s = %s.%s %s"
            (if isFirst then sprintf "%s as %s" parentTableName parentTableAlias else "")
            j.table
            j.alias
            parentTableAlias
            j.fkName
            j.alias
            j.pkName
            (List.map (computeFromClause j.table j.alias mapper false) jl
             |> String.concat "")

    let rec computeSelectClause (tp: System.Type) prefix (joins: compoundJoin) (mapper: IRecordMapper) =
        let j, jl = unroll joins

        (+)
            (FSharpType.GetRecordFields tp
             |> List.ofArray
             |> List.map (fun e -> prefix + "." + (mapper.MapField e))
             |> String.concat ", ")
            (List.map (fun e -> 
                        let nestedJoin, _ = unroll e
                        computeSelectClause nestedJoin.definingType nestedJoin.alias e mapper) jl
             |> String.concat ", ")

    let rec getValue (joins: compoundJoin list) (mapper: IRecordMapper) = function
    | PropertyGet (target, prop, idx) -> 
        match prop.PropertyType with
        | Record -> getValue (List.find (fun j -> (unroll j |> fst).fkName = mapper.MapField prop) joins) mapper target.Value
        | Option inner -> getValue joins mapper target.Value
        | _ -> sprintf "%s.%s" j.alias j.fkName
    | Value (v, tp) -> convertFrom v tp mapper
    | _ as e -> failwith <| sprintf "%A is an unexpected element" e

