namespace Furious

module Join =
    open Microsoft.FSharp.Reflection

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open System.Reflection

    open Interfaces
    open TypeUtils
    open ValueUtils
    
    type join = {
        table: string
        alias: string
        fkName: string
        pkName: string
        definingMember: PropertyInfo
    }

    type compoundJoin =
    | Val of join * compoundJoin list

    let unroll = function
    | Val (j, jl) -> j, jl

    let rec computeJoins (tp: System.Type) (mapper: IRecordMapper) prefix =
        let computeSingleJoin (e: PropertyInfo) =
            Val ( { table = mapper.MapRecord e.PropertyType
                    alias = prefix + "_" + e.Name
                    fkName = mapper.MapField e
                    pkName = match mapper.GetPrimaryKeyName e.PropertyType with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" mapper)
                    definingMember = e },
                computeJoins (e.PropertyType) mapper (prefix + "_" + e.Name))

        FSharpType.GetRecordFields tp
        |> List.ofArray
        |> List.filter (fun e -> FSharpType.IsRecord e.PropertyType)
        |> List.map computeSingleJoin

    let rec private computeFromClauseInternal parentTableName parentTableAlias (mapper: IRecordMapper) isFirst (v: compoundJoin) = 
        let j, jl = unroll v
        sprintf "%s inner join %s as %s on %s.%s = %s.%s %s"
            (if isFirst then sprintf "%s as %s" parentTableName parentTableAlias else "")
            j.table
            j.alias
            parentTableAlias
            j.fkName
            j.alias
            j.pkName
            (List.map (computeFromClauseInternal j.table j.alias mapper false) jl
             |> String.concat "")

    let computeFromClause (tp: System.Type) prefix (mapper: IRecordMapper) (v: compoundJoin list) = 
        let tName = mapper.MapRecord tp
        List.mapi (fun idx elem -> computeFromClauseInternal tName prefix mapper (idx = 0) elem) v
        |> String.concat ""

    let rec computeSelectClause (tp: System.Type) prefix (joins: compoundJoin list) (mapper: IRecordMapper) =
        (@)
            (FSharpType.GetRecordFields tp
             |> List.ofArray
             |> List.map (fun e -> 
                            let name = mapper.MapField e
                            sprintf "%s.%s as %s%s" prefix name prefix name))
            (List.map (fun e -> 
                        let nestedJoin, joinList = unroll e
                        computeSelectClause nestedJoin.definingMember.PropertyType nestedJoin.alias joinList mapper) joins)
        |> String.concat ", "

    let rec invertPropertyPath = function
    | PropertyGet (e,_,_) as prop -> (invertPropertyPath e.Value) @ [ prop ]
    | Var (_) as var -> [ var ]
    | Value (_) as v -> [ v ]
    | _ as e -> failwith <| sprintf "%A is an unknown property path component" e

    let rec getValue prefix (joins: compoundJoin list) (mapper: IRecordMapper) = function
    | h::t ->
        match h with
        | PropertyGet (target, prop, idx) -> 
            match prop.PropertyType with
            | Record -> 
                let j, js = List.pick (fun elem ->
                                        let j, js = unroll elem
                                        if j.definingMember = prop then Some (j, js)
                                        else None) joins

                getValue j.alias js mapper t
            | Option inner -> getValue prefix joins mapper t
            | _ -> sprintf "%s.%s" prefix (mapper.MapField prop)
        | Var (e) -> getValue prefix joins mapper t
        | Value (v, tp) -> convertFrom v tp mapper
        | _ as e -> failwith <| sprintf "%A is an unexpected element" e
    | [] -> ""

