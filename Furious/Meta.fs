namespace Furious

module Meta =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Reflection

    open Union
    open Expression
    open Interfaces
    open RecordMapping

    type Datastore(?keyMapper:IRecordMapper) =
        let defaultMapper = 
            { new IRecordMapper with
                member x.MapRecord tp = tp.Name
                member x.MapField field = field.Name
                member x.GetPrimaryKeyName tp = Some (tp.Name + "Id") }

        let printExpr mode expr = printf "%s - %A" mode expr

        let notEmpty optExpr expr = match optExpr with | Some e -> e | None -> expr

        let rec traverse unions mapper altExpr = function
        | Let (var, valExpr, nextExpr) ->
            match nextExpr with
            | Lambda (v, expr) -> 
                match expr with 
                | Call _ -> traverse unions mapper (Some valExpr) expr
                | _ -> unions, "", None
            | _ -> unions, "", None
        | SpecificCall <@ (<|) @> (ex,types,f::s::t) -> 
            let _,_,c =
                match notEmpty altExpr f with
                | Lambda (v, expr) -> 
                      traverse unions mapper altExpr expr
                | _ -> unions, "", None
            let u,e,_ = traverse unions mapper altExpr s
            u,e,c
        | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
            match notEmpty altExpr h with
            | Lambda (v, expr) -> 
                let u,e = traverseExpression unions mapper expr
                u,e,None
            | _ -> unions, "", None
        | SpecificCall <@ Seq.length @> (ex,types,h::t) ->  unions, "", Some "length"
        | ShapeVar v -> unions, "", None
        | ShapeLambda (v,expr) -> traverse unions mapper None expr
        | ShapeCombination (o, exprs) -> 
            List.fold (fun (u,e,c) expr -> traverse unions mapper None expr) (unions, "", None) exprs

        let computeFieldNames recType alias (mapper: IRecordMapper) =
            FSharpType.GetRecordFields (recType, System.Reflection.BindingFlags.Public ||| System.Reflection.BindingFlags.Instance)
            |> Array.toList
            |> List.map (fun e -> sprintf "%s.%s" alias (mapper.MapField e))
            |> List.fold (fun s e -> (if System.String.IsNullOrEmpty(s) then ", " else "") + e) ""

        member private x.Mapper with get() = match keyMapper with | Some m -> m | None -> defaultMapper
        member x.Yield (expr: Expr<(seq<'a>->seq<'b>)>) = 
            let newUnions,e,collation = traverse Map.empty x.Mapper None expr
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
                        |> List.fold (+) "")

            printfn "select %s from %s %s" select from (if System.String.IsNullOrWhiteSpace e then "" else "where " + e)
            Seq.empty<'b>
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
            writeRecord x.Mapper false record