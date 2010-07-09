namespace Furious

module Expression =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Join
    open Interfaces

    let rec traverseExpression prefix (joins: compoundJoin list) mapper expr = 
        let left, right, opString,leaf = 
            match expr with
            | IfThenElse (i,t,e) ->
                match t,e with
                // if a then b or false - a and b
                | _, Value (v1,tp) when v1 = (false :> obj) -> i,t,"and",false
                // if a then true or b - a or b
                | Value (v1,tp), _  when v1 = (true :> obj) -> i,e,"or",false
                | _ -> failwith <| sprintf "%A is an unexpected element" expr
            | SpecificCall <@ (||) @> (ex, types, f::l::[]) -> f,l,"or",false
            | SpecificCall <@ (&&) @> (ex, types, f::l::[]) -> f,l,"and",false
            | SpecificCall <@ (not) @> (ex, types, f::l::[]) -> f,l,"not",false
            | SpecificCall <@ (= ) @> (ex, types, f::l::[]) -> f,l,"=",true
            | SpecificCall <@ (< ) @> (ex, types, f::l::[]) -> f,l,"<",true
            | SpecificCall <@ (> ) @> (ex, types, f::l::[]) -> f,l,">",true
            | SpecificCall <@ (<=) @> (ex, types, f::l::[]) -> f,l,">=",true
            | SpecificCall <@ (>=) @> (ex, types, f::l::[]) -> f,l,"<=",true
            | _ as e -> failwith <| sprintf "%A is an unexpected element" e

        if not leaf then
            let newLeft = traverseExpression prefix joins mapper left
            let newRight = traverseExpression prefix joins mapper right
            sprintf "(%s) %s (%s)" newLeft opString newRight
        else
            let alias1 = 
                invertPropertyPath left
                |> getValue prefix joins mapper
            
            let alias2 = 
                invertPropertyPath right
                |> getValue prefix joins mapper

            alias1 + opString + alias2

    let notEmpty optExpr expr = match optExpr with | Some e -> e | None -> expr

    let rec traverse prefix joins mapper altExpr = function
    | Let (var, valExpr, nextExpr) ->
        match nextExpr with
        | Lambda (v, expr) -> 
            match expr with 
            | Call _ -> traverse prefix joins mapper (Some valExpr) expr
            | _ -> "", None
        | _ -> "", None
    | SpecificCall <@ (<|) @> (ex,types,f::s::t) -> 
        let _,c =
            match notEmpty altExpr f with
            | Lambda (v, expr) -> 
                    traverse prefix joins mapper altExpr expr
            | _ -> "", None
        let e,_ = traverse prefix joins mapper altExpr s
        e,c
    | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
        match notEmpty altExpr h with
        | Lambda (v, expr) -> 
            let e = traverseExpression prefix joins mapper expr
            e,None
        | _ -> "", None
    | SpecificCall <@ Seq.length @> (ex,types,h::t) ->  "", Some "length"
    | ShapeVar v -> "", None
    | ShapeLambda (v,expr) -> traverse prefix joins mapper None expr
    | ShapeCombination (o, exprs) -> 
        List.fold (fun (e,c) expr -> traverse prefix joins mapper None expr) ("", None) exprs

