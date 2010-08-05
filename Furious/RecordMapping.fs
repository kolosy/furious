namespace Furious

module RecordMapping =
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Collections

    open System
    open System.Reflection
    open System.Data

    open Interfaces
    open TypeUtils
    open ValueUtils

    let computeFieldNames recType alias (mapper: IRecordMapper) =
        FSharpType.GetRecordFields (recType)
        |> Array.map (fun e -> sprintf "%s.%s" alias (mapper.MapField e))
        |> String.concat ", "

    let rec readRecord recordType prefix (mapper: IRecordMapper) (reader: System.Data.Common.DbDataReader) parentIdField isSeq =
        if FSharpType.IsRecord recordType then
            let constrValues = 
                FSharpType.GetRecordFields (recordType)
                |> Array.map (fun (elem: PropertyInfo) ->
                    match elem.PropertyType with
                    | Sequence ->
                        let nestedType = elem.PropertyType.GetGenericArguments().[0]
                        ((readRecord nestedType (prefix + "_" + elem.Name + "Seq") mapper reader (Some (prefix + mapper.GetPrimaryKeyName(recordType).Value)) true) 
                         |> asTypedList nestedType)
                    | Record -> 
                        (readRecord elem.PropertyType (prefix + "_" + elem.Name) mapper reader (Some (prefix + mapper.GetPrimaryKeyName(recordType).Value)) false) 
                        |> List.head
                    | Option (tp) -> 
                        match tp with
                        | Record ->
                            if reader.GetValue(reader.GetOrdinal(prefix + "_" + elem.Name + mapper.GetPrimaryKeyName(tp).Value)) = (box DBNull.Value) then
                                createNone tp
                            else
                                List.head (readRecord tp prefix mapper reader parentIdField false)
                                |> createSome tp
                        | _ -> 
                            if reader.GetValue(reader.GetOrdinal(prefix + mapper.MapField(elem))) = (box DBNull.Value) then
                                createNone tp
                            else
                                List.head (readRecord tp prefix mapper reader parentIdField false)
                                |> createSome tp
                    | _ ->
                        let value = reader.GetValue(reader.GetOrdinal(prefix + mapper.MapField(elem)))
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
                        newRecord :: readRecord recordType prefix mapper reader parentIdField true
                    else
                        [newRecord]
                else
                    [newRecord]
            | _ -> 
                [newRecord]
        else
            [System.Convert.ChangeType(reader.GetValue(0), recordType)]

    let rec writeRecord (mapper:IRecordMapper) isInsert record =
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
            let id1, id2 = getId elem mapper, getId record mapper
            [ sprintf "insert into %s (%s, %s) values (%s, %s)" 
                (mapper.MapRecord (prop.PropertyType, prop)) 
                (mapper.GetPrimaryKeyName (elem.GetType())).Value
                (mapper.GetPrimaryKeyName (prop.DeclaringType)).Value
                (convertFrom id1 (id1.GetType()) mapper) 
                (convertFrom id2 (id2.GetType()) mapper);
              writeRecord mapper isInsert elem ]

        let rec generator currentRec (elem: PropertyInfo) = 
            match elem.PropertyType with
            | Sequence -> 
                List.collect (writeSeqElem elem) ((elem.GetValue(currentRec, null) :?> seq<_>) |> Seq.toList)
            | Record -> [writeRecord mapper isInsert (elem.GetValue(currentRec, null))]
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
                 |> String.concat ";\r\n") with
            | "" -> ""
            | _ as vSql -> vSql + ";\r\n"

        let names,values = 
            scalar
            |> List.map (fun prop -> mapper.MapField prop, convertFrom (prop.GetValue(record, null)) (prop.PropertyType) mapper)
            |> List.unzip

        sprintf "%s insert into %s (%s) values (%s)%s"
            vectorSql
            (mapper.MapRecord (record.GetType()))
            (String.concat ", " names)
            (String.concat ", " values)
            (match vectorSql with | "" -> "" | _ -> ";")