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
    
    type direction =
    | Inner
    | Left
    | Right

    type join = {
        table: string
        alias: string
        fkName: string
        pkName: string
        direction: direction
        selectable: bool
        definingMember: PropertyInfo
    }

    type compoundJoin =
    | Val of join * compoundJoin list

    let unroll = function
    | Val (j, jl) -> j, jl

    let rec computeJoins (tp: System.Type) (mapper: IRecordMapper) prefix =
        let computeSingleJoin (e: PropertyInfo) =
            // todo: this encodes a specific one:many table structure. will need to revisit.
                match e.PropertyType with
                | Sequence ->
                    let fkName = match mapper.GetPrimaryKeyName e.DeclaringType with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" mapper)
                    let j = 
                        { table = mapper.MapRecord(e.PropertyType, e)
                          alias = prefix + "_" + e.Name
                          fkName = fkName
                          pkName = fkName
                          selectable = false
                          direction = Left
                          definingMember = e }
                    
                    let nestedType = (tryGetNestedType e.PropertyType).Value
                    let pkName = match mapper.GetPrimaryKeyName nestedType with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" mapper)
                    let newPrefix = prefix + "_" + e.Name + "Seq"
                    let j2 = 
                        { table = mapper.MapRecord(nestedType)
                          alias = newPrefix
                          fkName = pkName
                          pkName = pkName
                          direction = Left
                          selectable = true

                          definingMember = e }

                    Val(j, [ Val(j2, computeJoins (nestedType) mapper newPrefix )])
                | _ ->
                    let j = 
                        { table = mapper.MapRecord(e.PropertyType, e)
                          alias = prefix + "_" + e.Name
                          fkName = mapper.MapField e
                          pkName = match mapper.GetPrimaryKeyName e.PropertyType with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" mapper)
                          selectable = true
                          direction = match e.PropertyType with | Option (_) -> Left | _ -> Inner
                          definingMember = e }
                    Val(j, computeJoins (e.PropertyType) mapper (prefix + "_" + e.Name))

        match tryGetNestedType tp with
        | Some t -> 
            FSharpType.GetRecordFields t
            |> List.ofArray
            |> List.filter (fun e -> match e.PropertyType with | Record | Sequence | Option (_) -> true | _ -> false)
            |> List.map computeSingleJoin
        | None -> failwith (sprintf "%A is an unsupported type" tp)

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

    let rec computeSelectClause (tp: System.Type) prefix (joins: compoundJoin list) (mapper: IRecordMapper) selectable =
        match tryGetNestedType tp with
        | Some t -> 
            (@)
                (if (not selectable) then []
                 else
                    (FSharpType.GetRecordFields t
                     |> List.ofArray
                     |> List.filter (fun e -> match e.PropertyType with | Sequence -> false | _ -> true )
                     |> List.map (fun e -> 
                                    let name = mapper.MapField e
                                    sprintf "%s.%s as %s%s" prefix name prefix name)))
                (List.map (fun e -> 
                            let nestedJoin, joinList = unroll e
                            computeSelectClause nestedJoin.definingMember.PropertyType nestedJoin.alias joinList mapper (nestedJoin.selectable)) joins)
            |> String.concat ", "
        | None -> failwith (sprintf "%A is an unsupported type" tp)

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

