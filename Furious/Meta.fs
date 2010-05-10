namespace Furious

module Meta =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns

    type unionMember = {
        target: string
        targetKey: string
        alias: string
    }

    type dbUnion = {
        left: unionMember
        right: unionMember
    }

    let printExpr mode expr = printf "%s - %A" mode expr

    // start with person.homeAddress.zip with homeAddress being of type address
    // get to person.homeAddress[Id] = address.addressId

    let rec getPropertyPath = function
    | PropertyGet (target, prop, idx) -> 
        let remainder = 
            match target with 
            | Some e -> getPropertyPath e
            | None -> failwith "static properties are not supported"
        remainder @ [prop.PropertyType.Name, prop.Name]
    | Var (e) ->  [ e.Type.Name, "" ]
    | _ as e -> failwith <| sprintf "%A is an unsupported element" e

    let rec generateAlias (name: string) unions = 
       (name.[0..1]) + (List.length unions).ToString()

    let rec buildUnionPath unions lastMbr = function
    | (tp,name)::[] -> 
        match lastMbr with
        | Some mbr -> mbr.alias, unions
        | None -> failwith "union path shorter than expected"
    | (tp,name)::t -> 
        match lastMbr with
        | Some mbr -> 
            let right = { target = tp; targetKey = tp + "Id"; alias = generateAlias tp unions }
            buildUnionPath ({ left = { mbr with targetKey = name }; right = right } :: unions) (Some right) t
        | None ->
            buildUnionPath unions (Some { target = tp; targetKey = name; alias = generateAlias tp unions }) t
    | [] -> failwith "no union path to compute"

    let rec last = function
    | h::[] -> h
    | h::t -> last t
    | _ -> failwith "empty list"

    let computeExpression expr unions = 
        let path = getPropertyPath expr
        if List.length path <= 2 then 
            unions, 
                ( match path with | f::s::_ -> fst f + "." + snd s | _ -> failwith "malformed property path" )
        else
            let alias, newUnions = buildUnionPath unions None path
            newUnions, alias + "." + (snd <| last path)

    let rec getValue unions = function
    | Value (v, tp) -> unions, v.ToString()
    | _ as expr -> computeExpression expr unions

    let rec traverseFilter unions expr = 
        let left, right, opString = 
            match expr with
            | SpecificCall <@ (= ) @> (ex, types, f::l::[]) -> f,l,"="
            | SpecificCall <@ (< ) @> (ex, types, f::l::[]) -> f,l,"<"
            | SpecificCall <@ (> ) @> (ex, types, f::l::[]) -> f,l,">"
            | SpecificCall <@ (<=) @> (ex, types, f::l::[]) -> f,l,">="
            | SpecificCall <@ (>=) @> (ex, types, f::l::[]) -> f,l,"<="
            | _ as e -> failwith <| sprintf "%A is an unexpected element" e

        let newUnions1, alias1 = getValue unions left
        let newUnions2, alias2 = getValue newUnions1 right
        newUnions2, alias1 + opString + alias2

    let printUnion (union: dbUnion)  =
        sprintf "%s %s inner join %s %s on %s.%s = %s.%s" 
            union.left.target
            union.left.alias
            union.right.target
            union.right.alias
            union.left.alias
            union.left.targetKey
            union.right.alias
            union.right.targetKey

    let printUnions unions = 
        List.fold (fun s e -> (if System.String.IsNullOrEmpty s then "" else ", ") + printUnion e) "" unions

    let rec traverse unions = function
    | SpecificCall <@ Seq.filter @> (ex,types,h::t) -> 
        match h with
        | Lambda (v, expr) -> 
            let newUnions, e = traverseFilter unions expr
            printfn "union: %s" (printUnions newUnions) |> ignore
            printfn "the expression is %s" e |> ignore
        | _ -> ()
    | ShapeVar v -> ()
    | ShapeLambda (v,expr) -> traverse unions expr
    | ShapeCombination (o, exprs) -> List.map (traverse unions) exprs |> ignore

    type Datastore() =
        member x.Yield (expr: Expr<(seq<'a>->seq<'b>)>) = 
            traverse [] expr
            Seq.empty<'b>
        member x.Compute (expr: Expr<(seq<'a>->'b)>) = 
            traverse [] expr
            Seq.empty<'b>
