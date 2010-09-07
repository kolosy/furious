namespace Furious

module Expression =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Join
    open Interfaces
    open Definitions

    let rec traverseExpression prefix (joins: compoundJoin list) context expr = 
        let left, right, opString,leaf = 
            match expr with
            | IfThenElse (i,t,e) ->
                match t,e with
                // if a then b or false - a and b
                | _, Value (v1,tp)  when v1 = (false :> obj) -> i,t,"and",false
                // if a then true or b - a or b
                | Value (v1,tp), _   when v1 = (true :> obj) -> i,e,"or",false
                | _ -> failwith <| sprintf "%A is an unexpected element" expr
            | SpecificCall <@ (|| ) @> (ex, types, f::l::[]) -> f,l,"or",false
            | SpecificCall <@ (&& ) @> (ex, types, f::l::[]) -> f,l,"and",false
            | SpecificCall <@ (not) @> (ex, types, f::l::[]) -> f,l,"not",false
            | SpecificCall <@ (=  ) @> (ex, types, f::l::[]) -> f,l,"=",true
            | SpecificCall <@ (<  ) @> (ex, types, f::l::[]) -> f,l,"<",true
            | SpecificCall <@ (>  ) @> (ex, types, f::l::[]) -> f,l,">",true
            | SpecificCall <@ (<= ) @> (ex, types, f::l::[]) -> f,l,">=",true
            | SpecificCall <@ (>= ) @> (ex, types, f::l::[]) -> f,l,"<=",true
            | _ as e -> failwith <| sprintf "%A is an unexpected element" e

        if not leaf then
            context.dialect.RecBoolean
                opString
                (traverseExpression prefix joins context left)
                (traverseExpression prefix joins context right)
        else
            context.dialect.Boolean
                opString
                (invertPropertyPath left |> getValue prefix joins context)
                (invertPropertyPath right |> getValue prefix joins context)

    let notEmpty optExpr expr = match optExpr with | Some e -> e | None -> expr

    let rec traverse prefix joins context altExpr = function
    | Let (var, valExpr, nextExpr) ->
        match nextExpr with
        | Lambda (v, expr) -> 
            match expr with 
            | Call _ -> traverse prefix joins context (Some valExpr) expr
            | _ -> emptyString, None
        | _ -> emptyString, None
    | SpecificCall <@ (<|) @> (ex,types,f::s::t) -> 
        let _,c =
            match notEmpty altExpr f with
            | Lambda (v, expr) -> 
                    traverse prefix joins context altExpr expr
            | _ -> emptyString, None
        let e,_ = traverse prefix joins context altExpr s
        e,c
    | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
        match notEmpty altExpr h with
        | Lambda (v, expr) -> 
            let e = traverseExpression prefix joins context expr
            e,None
        | _ -> emptyString, None
    | SpecificCall <@ Seq.length @> (ex,types,h::t) ->  emptyString, Some "length"
    | ShapeVar v -> emptyString, None
    | ShapeLambda (v,expr) -> traverse prefix joins context None expr
    | ShapeCombination (o, exprs) -> 
        List.fold (fun (e,c) expr -> traverse prefix joins context None expr) (emptyString, None) exprs

