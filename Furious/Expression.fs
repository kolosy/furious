namespace Furious

module Expression =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Union
    open Interfaces

    let printExpr mode expr = printf "%s - %A" mode expr

    // start with person.homeAddress.zip with homeAddress being of type address
    // get to person.homeAddress[Id] = address.addressId

    let rec traverseExpression unions mapper expr = 
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
            let newUnions1, newLeft = traverseExpression unions mapper left
            let newUnions2, newRight = traverseExpression newUnions1 mapper right
            newUnions2, sprintf "(%s) %s (%s)" newLeft opString newRight
        else
            let newUnions1, alias1 = getValue unions mapper left
            let newUnions2, alias2 = getValue newUnions1 mapper right
            newUnions2, alias1 + opString + alias2