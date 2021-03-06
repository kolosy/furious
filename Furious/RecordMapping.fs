﻿namespace Furious

module RecordMapping =
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Collections

    open System
    open System.Reflection
    open System.Data

    open Interfaces
    open TypeUtils
    open ValueUtils
    open Definitions

    let computeFieldNames recType alias (context: context) =
        FSharpType.GetRecordFields (recType)
        |> Array.map (fun e -> context.dialect.Qualify alias (context.mapper.MapField e))
        |> String.concat ", "

    let rec readRecord recordType prefix (context: context) (reader: System.Data.Common.DbDataReader) parentIdField isSeq =
        if FSharpType.IsRecord recordType then
            let constrValues = 
                FSharpType.GetRecordFields (recordType)
                |> Array.map (fun (elem: PropertyInfo) ->
                    match elem.PropertyType with
                    | Sequence ->
                        let nestedType = elem.PropertyType.GetGenericArguments().[0]
                        ((readRecord nestedType (prefix + "_" + elem.Name + "Seq") context reader (Some (prefix + context.mapper.GetPrimaryKeyName(recordType).Value)) true) 
                         |> asTypedList nestedType)
                    | Record -> 
                        (readRecord elem.PropertyType (prefix + "_" + elem.Name) context reader (Some (prefix + context.mapper.GetPrimaryKeyName(recordType).Value)) false) 
                        |> List.head
                    | Option (tp) -> 
                        match tp with
                        | Record ->
                            if reader.GetValue(reader.GetOrdinal(prefix + "_" + elem.Name + context.mapper.GetPrimaryKeyName(tp).Value)) = (box DBNull.Value) then
                                createNone tp
                            else
                                List.head (readRecord tp prefix context reader parentIdField false)
                                |> createSome tp
                        | _ -> 
                            if reader.GetValue(reader.GetOrdinal(prefix + context.mapper.MapField(elem))) = (box DBNull.Value) then
                                createNone tp
                            else
                                List.head (readRecord tp prefix context reader parentIdField false)
                                |> createSome tp
                    | _ ->
                        if (match context.mapper.GetTrackerName(recordType) with | None -> false | Some v -> v = elem.Name) then null
                        else
                            let value = reader.GetValue(reader.GetOrdinal(prefix + context.mapper.MapField(elem)))
                            try
                                Convert.ChangeType(value, elem.PropertyType)
                            with 
                            | :? InvalidCastException -> failwithf "Could not convert %A to %A" value elem.PropertyType)
                
            let newRecord = FSharpValue.MakeRecord (recordType, constrValues)
            
            match parentIdField, isSeq with
            | Some v, true -> 
                let idValue = reader.GetValue(reader.GetOrdinal(v))
                if reader.Read() then
                    let nextIdValue = reader.GetValue(reader.GetOrdinal(v))
                    if nextIdValue = idValue then
                        newRecord :: readRecord recordType prefix context reader parentIdField true
                    else
                        [newRecord]
                else
                    [newRecord]
            | _ -> 
                [newRecord]
        else
            [System.Convert.ChangeType(reader.GetValue(0), recordType)]

    let rec writeRecord (context: context) isInsert record =
        let fields = 
            FSharpType.GetRecordFields (record.GetType())
            |> Array.toList

        let filter = fun (flag: bool) (elem: PropertyInfo) -> 
                        match elem.PropertyType with 
                        | Sequence -> flag
                        | Record -> true 
                        | Option t -> match t with 
                                        | Sequence -> flag
                                        | Record -> true 
                                        | _ -> not flag
                        | _ -> not flag

        let vector = List.filter (filter true) fields
        let scalar = List.filter (filter false) fields

        // todo: this too encodes a specific 1:many structure. revisit.
        let writeSeqElem (prop: PropertyInfo) (elem: obj) = 
            let id1, id2 = getId elem context, getId record context
            [ context.dialect.InsertBridge 
                (context.mapper.MapRecord (prop.PropertyType, prop)) 
                (context.mapper.GetPrimaryKeyName (elem.GetType())).Value
                (context.mapper.GetPrimaryKeyName (prop.DeclaringType)).Value
                (convertFrom id1 (id1.GetType()) context) 
                (convertFrom id2 (id2.GetType()) context);
              writeRecord context isInsert elem ]

        let rec generator currentRec (elem: PropertyInfo) = 
            match elem.PropertyType with
            | Sequence -> 
                List.collect (writeSeqElem elem) ((elem.GetValue(currentRec, null) :?> seq<_>) |> Seq.toList)
            | Record -> [writeRecord context isInsert (elem.GetValue(currentRec, null))]
            | Option t -> 
                let info, values = FSharpValue.GetUnionFields(elem.GetValue(currentRec, null), elem.PropertyType)
                match info.Name with
                | "Some" -> generator (elem.GetValue(currentRec, null)) (info.GetFields().[0])
                | _ -> []
            | _ -> failwith (sprintf "%s is an unexpected type" (elem.PropertyType.Name))

        let vectorSql = 
            match 
                (vector 
                 |> List.collect (generator record)
                 |> String.concat context.dialect.BatchSep) with
            | "" -> emptyString
            | _ as vSql -> vSql + context.dialect.BatchSep

        let names,values = 
            scalar
            |> List.map (fun prop -> context.mapper.MapField prop, convertFrom (prop.GetValue(record, null)) (prop.PropertyType) context)
            |> List.unzip

        vectorSql + " " + 
            context.dialect.Insert 
                (context.mapper.MapRecord (record.GetType()))
                names
                values
                (String.IsNullOrWhiteSpace(vectorSql))