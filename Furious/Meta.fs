namespace Furious

module Meta =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Reflection

    open Join
    open Expression
    open Interfaces
    open RecordMapping

    type Datastore(conn: unit -> System.Data.Common.DbConnection, ?keyMapper:IRecordMapper) =
        let defaultMapper = 
            { new IRecordMapper with
                member x.MapRecord tp = tp.Name
                member x.MapField field = 
                    if FSharpType.IsRecord (match field with 
                                            | :? System.Reflection.PropertyInfo as pi -> pi.PropertyType
                                            | :? System.Reflection.FieldInfo as fi -> fi.FieldType 
                                            | _ -> failwith (sprintf "%A is an unsupported field descriptor" field) ) then
                        field.Name + "Id"
                    else
                        field.Name
                member x.GetPrimaryKeyName tp = Some (tp.Name + "Id") }

        let printExpr mode expr = printf "%s - %A" mode expr

        let notEmpty optExpr expr = match optExpr with | Some e -> e | None -> expr

        let rec traverse joins mapper altExpr = function
        | Let (var, valExpr, nextExpr) ->
            match nextExpr with
            | Lambda (v, expr) -> 
                match expr with 
                | Call _ -> traverse joins mapper (Some valExpr) expr
                | _ -> "", None
            | _ -> "", None
        | SpecificCall <@ (<|) @> (ex,types,f::s::t) -> 
            let _,c =
                match notEmpty altExpr f with
                | Lambda (v, expr) -> 
                      traverse joins mapper altExpr expr
                | _ -> "", None
            let e,_ = traverse joins mapper altExpr s
            e,c
        | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
            match notEmpty altExpr h with
            | Lambda (v, expr) -> 
                let e = traverseExpression joins mapper expr
                e,None
            | _ -> "", None
        | SpecificCall <@ Seq.length @> (ex,types,h::t) ->  "", Some "length"
        | ShapeVar v -> "", None
        | ShapeLambda (v,expr) -> traverse joins mapper None expr
        | ShapeCombination (o, exprs) -> 
            List.fold (fun (e,c) expr -> traverse joins mapper None expr) ("", None) exprs

        let computeFieldNames recType alias (mapper: IRecordMapper) =
            FSharpType.GetRecordFields (recType)
            |> Array.map (fun e -> sprintf "%s.%s" alias (mapper.MapField e))
            |> String.concat ", "

        member private x.Mapper with get() = match keyMapper with | Some m -> m | None -> defaultMapper
        
        member private x.RunSql sql =
            let command = conn().CreateCommand() 
            command.CommandText <- sql
            command.ExecuteReader()

        member x.Yield (expr: Expr<(seq<'a>->seq<'b>)>) = 
            let joins = computeJoins typeof<'b> x.Mapper ""
            let e,collation = traverse joins x.Mapper None expr
            let select = 
                match collation with 
                | Some e -> failwith (sprintf "unknown collation function %s" e)
                | None ->
                    computeFieldNames typeof<'b> (fst newUnions.[typeof<'b>.Name]).alias x.Mapper
            let from = (
                        Map.toList newUnions 
                        |> List.unzip 
                        |> snd 
                        |> computeFromClause []
                        |> String.concat "")

            let reader = 
                sprintf "select %s from %s %s" select from (if System.String.IsNullOrWhiteSpace e then "" else "where " + e)
                |> x.RunSql

            seq {
                while reader.HasRows do
                    for r in (readRecord typeof<'b> "" (x.Mapper) reader "") -> r :?> 'b
            }

        member x.Compute (expr: Expr<(seq<'a>->'b)>) = 
            let newUnions,e,collation = traverse Map.empty x.Mapper None expr
            let select = 
                match collation with 
                | Some "length" ->
                    sprintf "select: count(*)"
                | Some e -> failwith (sprintf "unknown collation function %s" e)
                | None ->
                    sprintf "select: %A" (computeFieldNames typeof<'b> (fst newUnions.[typeof<'b>.Name]).alias x.Mapper)
            let from = (
                        Map.toList newUnions 
                        |> List.unzip 
                        |> snd 
                        |> computeFromClause []
                        |> List.fold (+) "")

            printfn "select %s from %s %s" select from (if System.String.IsNullOrWhiteSpace e then "" else "where " + e)
            Seq.empty<'b>

        member x.Save record =
            let cmd = conn().CreateCommand()
            cmd.CommandText <- writeRecord x.Mapper false record
            cmd.CommandType <- System.Data.CommandType.Text
            cmd.ExecuteNonQuery()