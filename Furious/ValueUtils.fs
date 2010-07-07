﻿namespace Furious

module ValueUtils =
    open Microsoft.FSharp.Reflection

    open Interfaces
    open TypeUtils

    let getId (record: obj) (mapper: IRecordMapper) =
        FSharpValue.GetRecordField(
            record,
            FSharpType.GetRecordFields(record.GetType())
            |> Array.pick (fun elem -> if elem.Name = 
                                            (match (mapper.GetPrimaryKeyName <| record.GetType()) with
                                             | Some v -> v | None -> failwith (sprintf "%A doesn't have an id field" (record.GetType()))) 
                                       then Some elem 
                                       else None))

    let rec convertFrom (record: obj) (tp: System.Type) (mapper: IRecordMapper) = 
        match record with
        | :? string as str -> "'" + str + "'"
        | _ when isOption (tp) ->
            match record with
            | AsOption tp opt -> convertFrom opt (opt.GetType()) mapper
            | _ -> "null"
        | _ when FSharpType.IsRecord tp -> 
            let fk = getId record mapper
            convertFrom fk (fk.GetType()) mapper
        | _ as v -> v.ToString()

    let rec convertTo targetType (v: obj) (mapper: IRecordMapper) =
        if typeof<System.Enum>.IsAssignableFrom(targetType) then
            System.Enum.ToObject(targetType, v :?> System.Int64)
        elif targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typeof<Option<_>> then
            if v = (System.DBNull.Value :> obj) then None :> obj
            else convertTo (targetType.GetGenericArguments().[0]) v mapper
        else
            System.Convert.ChangeType(v,targetType)
