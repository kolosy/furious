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
        | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
            match h with
            | Lambda (v, expr) -> 
                let newUnions, e = traverseExpression unions mapper expr
                printfn "union: %A" newUnions |> ignore
                printfn "the expression: %s" e |> ignore
                printfn "from clause: %s" (
                                            Map.toList newUnions 
                                            |> List.unzip 
                                            |> snd 
                                            |> computeFromClause []
                                            |> List.fold (+) "")
            | _ -> ()
        | ShapeVar v -> ()
        | ShapeLambda (v,expr) -> traverse unions mapper expr
        | ShapeCombination (o, exprs) -> List.map (traverse unions mapper) exprs |> ignore

        member private x.Mapper with get() = match keyMapper with | Some m -> m | None -> defaultMapper
        member x.Yield (expr: Expr<(seq<'a>->seq<'b>)>) = 
            traverse Map.empty x.Mapper expr
            Seq.empty<'b>
        member x.Compute (expr: Expr<(seq<'a>->'b)>) = 
            traverse Map.empty x.Mapper expr
            Seq.empty<'b>