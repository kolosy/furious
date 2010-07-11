namespace Furious

module TypeUtils =
    open Microsoft.FSharp.Reflection

    let isOption (tp: System.Type) = tp.IsGenericType && tp.GetGenericTypeDefinition() = typeof<Option<_>>.GetGenericTypeDefinition()

    let (|Option|_|) (tp: System.Type) = if isOption tp then Some(tp.GetGenericArguments().[0]) else None
    let (|Record|_|) (tp: System.Type) = if FSharpType.IsRecord tp then Some() else None
    let (|Sequence|_|) (tp: System.Type) = 
        if tp.IsGenericType && typeof<System.Collections.IEnumerable>.IsAssignableFrom(tp.GetGenericTypeDefinition()) then Some() else None

    let (|AsOption|_|) (tp: System.Type) (o: obj) =
        let info,values = FSharpValue.GetUnionFields(o, tp)
        match info.Name with
        | "Some" -> Some (values.[0])
        | "None" -> None
        | _ -> failwith (sprintf "unkown case %s" info.Name)

    let createSome (tp: System.Type) o =
        let optType = typeof<Option<_>>.GetGenericTypeDefinition().MakeGenericType([|tp|])
        FSharpValue.MakeUnion(FSharpType.GetUnionCases(optType) |> Array.find (fun elem -> elem.Name = "Some"), [| o |])
    
    let createNone (tp: System.Type) =
        let optType = typeof<Option<_>>.GetGenericTypeDefinition().MakeGenericType([|tp|])
        FSharpValue.MakeUnion(FSharpType.GetUnionCases(optType) |> Array.find (fun elem -> elem.Name = "None"), Array.empty)

    let tryGetNestedType = function
    | Record as tp -> Some tp
    | Option (t) -> Some t
    | Sequence as tp -> Some(tp.GetGenericArguments().[0])
    | _ -> None