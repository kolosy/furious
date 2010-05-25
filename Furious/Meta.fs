namespace Furious

module Meta =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Union
    open Expression
    open Interfaces

    type Datastore(?keyMapper:IRecordMapper) =
        let defaultMapper = 
            { new IRecordMapper with
                member x.MapRecord tp = tp.Name
                member x.MapField field = field.Name
                member x.GetPrimaryKeyName tp = Some (tp.Name + "Id") }

        let printExpr mode expr = printf "%s - %A" mode expr

        let rec traverse unions mapper = function
        | Let (var, valExpr, nextExpr) ->
            match nextExpr with
            | Lambda (v, expr) -> 
                match expr with 
                | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
                    match valExpr with
                    | Lambda (v, expr) -> traverseExpression unions mapper expr
                    | _ -> unions, ""
                | _ -> unions, ""
            | _ -> unions, ""
        | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
            match h with
            | Lambda (v, expr) -> traverseExpression unions mapper expr
            | _ -> unions, ""
        | ShapeVar v -> unions, ""
        | ShapeLambda (v,expr) -> traverse unions mapper expr
        | ShapeCombination (o, exprs) -> 
            List.fold (fun (u,e) expr -> traverse unions mapper expr) (unions, "") exprs

        member private x.Mapper with get() = match keyMapper with | Some m -> m | None -> defaultMapper
        member x.Yield (expr: Expr<(seq<'a>->seq<'b>)>) = 
            let newUnions,e = traverse Map.empty x.Mapper expr
            printfn "union: %A" newUnions |> ignore
            printfn "the expression: %s" e |> ignore
            printfn "from clause: %s" (
                                        Map.toList newUnions 
                                        |> List.unzip 
                                        |> snd 
                                        |> computeFromClause []
                                        |> List.fold (+) "")
            Seq.empty<'b>
        member x.Compute (expr: Expr<(seq<'a>->'b)>) = 
            let newUnions,e = traverse Map.empty x.Mapper expr
            printfn "union: %A" newUnions |> ignore
            printfn "the expression: %s" e |> ignore
            printfn "from clause: %s" (
                                        Map.toList newUnions 
                                        |> List.unzip 
                                        |> snd 
                                        |> computeFromClause []
                                        |> List.fold (+) "")
            Seq.empty<'b>