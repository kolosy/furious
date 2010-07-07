namespace Furious

module Expression =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Join
    open Interfaces

    let rec traverseExpression (joins: compoundJoin) mapper expr = 
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
            let newLeft = traverseExpression joins mapper left
            let newRight = traverseExpression joins mapper right
            sprintf "(%s) %s (%s)" newLeft opString newRight
        else
            let alias1 = getValue joins mapper left
            let alias2 = getValue joins mapper right
            alias1 + opString + alias2