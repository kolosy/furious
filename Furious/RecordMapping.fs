namespace Furious

module RecordMapping =
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Collections

    open System.Reflection
    open System.Data

    open Interfaces
    open TypeUtils

    let rec convertFrom (record: obj) (tp: System.Type) = 
        match record with
        | :? string as str -> "'" + str + "'"
        | _ when isOption (tp) ->
            match record with
            | AsOption tp opt -> convertFrom opt (opt.GetType())
            | _ -> "null"
        | _ as v -> v.ToString()

    let rec convertTo targetType (v: obj) =
        if typeof<System.Enum>.IsAssignableFrom(targetType) then
            System.Enum.ToObject(targetType, v :?> System.Int64)
        elif targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typeof<Option<_>> then
            if v = (System.DBNull.Value :> obj) then None :> obj
            else convertTo (targetType.GetGenericArguments().[0]) v
        else
            System.Convert.ChangeType(v,targetType)

    let rec readRecord recordType prefix (mapper: IRecordMapper) (reader: System.Data.Common.DbDataReader) (parentIdField: string) =
        let constrValues = 
            FSharpType.GetRecordFields (recordType)
            |> Array.map (fun (elem: PropertyInfo) ->
                match elem.PropertyType with
                | Sequence ->
                    (readRecord elem.PropertyType (prefix + elem.Name + "_") mapper reader (prefix + mapper.GetPrimaryKeyName(recordType).Value)) :> obj
                | Record -> 
                    ((readRecord elem.PropertyType (prefix + elem.Name + "_") mapper reader (prefix + mapper.GetPrimaryKeyName(recordType).Value)) |> List.head) :> obj
                | _ ->
                    System.Convert.ChangeType(reader.GetValue(reader.GetOrdinal(prefix + mapper.MapField(elem))), elem.PropertyType))
                
        let newRecord = FSharpValue.MakeRecord (recordType, constrValues)
        let idValue = reader.GetValue(reader.GetOrdinal(parentIdField))
        if reader.Read() then
            let nextIdValue = reader.GetValue(reader.GetOrdinal(parentIdField))
            if nextIdValue = idValue then
                newRecord :: readRecord recordType prefix mapper reader parentIdField
            else
                [newRecord]
        else
            [newRecord]

    let rec writeRecord (mapper:IRecordMapper) isInsert record =
        let vector,scalar = 
            FSharpType.GetRecordFields (record.GetType())
            |> Array.toList
            |> List.partition (fun (elem: PropertyInfo) -> 
                                match elem.PropertyType with 
                                | Sequence | Record -> true 
                                | Option t -> match t with | Sequence | Record -> true | _ -> false
                                | _ -> false)

        let rec generator currentRec (elem: PropertyInfo) = 
            match elem.PropertyType with
            | Sequence -> Seq.map (writeRecord mapper isInsert) (elem.GetValue(currentRec, null) :?> seq<_>) |> Seq.toList
            | Record -> [writeRecord mapper isInsert (elem.GetValue(currentRec, null))]
            | Option t -> 
                let info, values = FSharpValue.GetUnionFields(elem.GetValue(currentRec, null), elem.PropertyType)
                match info.Name with
                | "Some" -> generator (elem.GetValue(currentRec, null)) (info.GetFields().[0])
                | _ -> []
            | _ -> failwith (sprintf "%s is an unexpected type" (elem.PropertyType.Name))

        let vectorSql = 
            vector 
            |> List.collect (generator record)
            |> String.concat "\r\n go \r\n"

        let names,values = 
            scalar
            |> List.map (fun prop -> mapper.MapField prop, convertFrom (prop.GetValue(record, null)) (prop.PropertyType))
            |> List.unzip

        sprintf "%s\r\n insert into %s (%s) values (%s)"
            vectorSql
            (mapper.MapRecord (record.GetType()))
            (String.concat ", " names)
            (String.concat ", " values)