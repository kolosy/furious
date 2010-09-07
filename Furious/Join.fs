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
    open Definitions
    
    type compoundJoin =
    | Val of join * compoundJoin list

    let unroll = function
    | Val (j, jl) -> j, jl

    let rec computeJoins (tp: System.Type) (context: context) prefix =
        let computeSingleJoin (e: PropertyInfo) =
            // todo: this encodes a specific one:many table structure. will need to revisit.
                match e.PropertyType with
                | Sequence ->
                    let fkName = match context.mapper.GetPrimaryKeyName e.DeclaringType with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" context)
                    let j = 
                        { table = context.mapper.MapRecord(e.PropertyType, e)
                          alias = prefix + "_" + e.Name
                          fkName = fkName
                          pkName = fkName
                          selectable = false
                          direction = Left
                          definingMember = e }
                    
                    let nestedType = (tryGetNestedType e.PropertyType).Value
                    let pkName = match context.mapper.GetPrimaryKeyName nestedType with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" context)
                    let newPrefix = prefix + "_" + e.Name + "Seq"
                    let j2 = 
                        { table = context.mapper.MapRecord(nestedType)
                          alias = newPrefix
                          fkName = pkName
                          pkName = pkName
                          direction = Left
                          selectable = true

                          definingMember = e }

                    Val(j, [ Val(j2, computeJoins (nestedType) context newPrefix )])
                | _ ->
                    let j = 
                        { table = context.mapper.MapRecord(e.PropertyType, e)
                          alias = prefix + "_" + e.Name
                          fkName = context.mapper.MapField e
                          pkName = match context.mapper.GetPrimaryKeyName e.PropertyType with | Some v -> v | None -> failwith (sprintf "Mapper %A needs to support primary key inferrence" context)
                          selectable = true
                          direction = match e.PropertyType with | Option (_) -> Left | _ -> Inner
                          definingMember = e }
                    Val(j, computeJoins (e.PropertyType) context (prefix + "_" + e.Name))

        match tryGetNestedType tp with
        | Some t -> 
            FSharpType.GetRecordFields t
            |> List.ofArray
            |> List.filter (fun e -> match e.PropertyType with | Record | Sequence | Option (_) -> true | _ -> false)
            |> List.map computeSingleJoin
        | None -> failwith (sprintf "%A is an unsupported type" tp)

    let rec private computeFromClauseInternal parentTableName parentTableAlias (context: context) isFirst (v: compoundJoin) = 
        let j, jl = unroll v

        context.dialect.Join
            isFirst
            j.direction
            parentTableName
            parentTableAlias
            j.table
            j.alias
            j.pkName
            j.fkName
            (List.map (computeFromClauseInternal j.table j.alias context false) jl
                |> String.concat emptyString)

    let computeFromClause (tp: System.Type) prefix (context: context) (v: compoundJoin list) = 
        let tName = context.mapper.MapRecord tp
        List.mapi (fun idx elem -> computeFromClauseInternal tName prefix context (idx = 0) elem) v
        |> String.concat emptyString

    let rec computeSelectClause (tp: System.Type) prefix (joins: compoundJoin list) (context: context) selectable =
        match tryGetNestedType tp with
        | Some t -> 
            (@)
                (if (not selectable) then []
                 else
                    (FSharpType.GetRecordFields t
                     |> List.ofArray
                     |> List.filter (fun e -> match e.PropertyType with | Sequence -> false | _ -> true )
                     |> List.map (fun e -> 
                                    let name = context.mapper.MapField e
                                    context.dialect.AliasColumn prefix name (prefix+name))))
                (List.map (fun e -> 
                            let nestedJoin, joinList = unroll e
                            computeSelectClause nestedJoin.definingMember.PropertyType nestedJoin.alias joinList context (nestedJoin.selectable)) joins)
            |> String.concat ", "
        | None -> failwith (sprintf "%A is an unsupported type" tp)

    let rec invertPropertyPath = function
    | PropertyGet (e,_,_) as prop -> (invertPropertyPath e.Value) @ [ prop ]
    | Var (_) as var -> [ var ]
    | Value (_) as v -> [ v ]
    | _ as e -> failwith <| sprintf "%A is an unknown property path component" e

    let rec getValue prefix (joins: compoundJoin list) (context: context) = function
    | h::t ->
        match h with
        | PropertyGet (target, prop, idx) -> 
            match prop.PropertyType with
            | Record -> 
                let j, js = List.pick (fun elem ->
                                        let j, js = unroll elem
                                        if j.definingMember = prop then Some (j, js)
                                        else None) joins

                getValue j.alias js context t
            | Option inner -> getValue prefix joins context t
            | _ -> context.dialect.Value prefix (context.mapper.MapField prop)
        | Var (e) -> getValue prefix joins context t
        | Value (v, tp) -> convertFrom v tp context
        | _ as e -> failwith <| sprintf "%A is an unexpected element" e
    | [] -> emptyString