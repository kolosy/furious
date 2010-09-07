namespace Furious

module ValueUtils =
    open Microsoft.FSharp.Reflection

    open Interfaces
    open TypeUtils

    let getId (record: obj) (context: context) =
        FSharpValue.GetRecordField(
            record,
            FSharpType.GetRecordFields(record.GetType())
            |> Array.pick (fun elem -> if elem.Name = 
                                            (match (context.mapper.GetPrimaryKeyName <| record.GetType()) with
                                             | Some v -> v | None -> failwith (sprintf "%A doesn't have an id field" (record.GetType()))) 
                                       then Some elem 
                                       else None))

    let rec convertFrom (record: obj) (tp: System.Type) (context: context) = 
        match record with
        | :? string as str -> context.dialect.String str
        | _ when isOption (tp) ->
            match record with
            | AsOption tp opt -> convertFrom opt (opt.GetType()) context
            | _ -> context.dialect.NullValue
        | _ when FSharpType.IsRecord tp -> 
            let fk = getId record context
            convertFrom fk (fk.GetType()) context
        | _ as v -> v.ToString()

    let rec convertTo targetType (v: obj) (context: context) =
        if typeof<System.Enum>.IsAssignableFrom(targetType) then
            System.Enum.ToObject(targetType, v :?> System.Int64)
        elif targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typeof<Option<_>> then
            if v = (System.DBNull.Value :> obj) then None :> obj
            else convertTo (targetType.GetGenericArguments().[0]) v context
        else
            System.Convert.ChangeType(v,targetType)

    let ucFirst (str: string) = str.[0].ToString().ToUpper() + str.[1..]

    let asTypedList (tp: System.Type) (lst: obj list) = 
        let lstTp = typeof<Microsoft.FSharp.Collections.list<_>>.GetGenericTypeDefinition().MakeGenericType([| tp |])
        let lstConstructor = lstTp.GetConstructor([| tp; lstTp |])

        let rec computeList = function
        | h::t -> lstConstructor.Invoke([|System.Convert.ChangeType(h, tp); computeList t|])
        | [] -> lstTp.GetProperty("Empty").GetValue(null, null)

        computeList lst

    let stateProperty (r: obj) (context: context) =
        match context.mapper.GetTrackerName(r.GetType()) with
        | Some s -> Some (r.GetType().GetProperty(s))
        | None -> None
