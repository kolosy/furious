namespace Furious

module RecordMapping =
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Collections

    open System.Reflection
    open System.Data

    open Interfaces

    let isSeq (tp: System.Type) = 
        tp.IsGenericType &&
        typeof<System.Collections.IEnumerable>.IsAssignableFrom(tp.GetGenericTypeDefinition())

    let rec getSafeValue record (prop: PropertyInfo) = 
        match prop.GetValue(record, null) with
        | :? string as str -> "'" + str + "'"
        | :? Option<_> as opt -> 
            match opt with
            | Some v -> getSafeValue v prop
            | None -> "null"
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
                        if isSeq (elem.PropertyType) then
                            (readRecord elem.PropertyType (prefix + elem.Name + "_") mapper reader (prefix + mapper.GetPrimaryKeyName(recordType).Value)) :> obj
                        elif FSharpType.IsRecord elem.PropertyType then
                            ((readRecord elem.PropertyType (prefix + elem.Name + "_") mapper reader (prefix + mapper.GetPrimaryKeyName(recordType).Value)) |> List.head) :> obj
                        else
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

    let rec writeRecord record (mapper:IRecordMapper) isInsert =
        let vector,scalar = 
            FSharpType.GetRecordFields (record.GetType())
            |> Array.toList
            |> List.partition (fun (elem: PropertyInfo) ->
                        isSeq (elem.PropertyType)
                        || FSharpType.IsRecord elem.PropertyType)

        let vectorSql = 
            vector 
            |> List.collect
                    (fun elem ->
                            if isSeq (elem.PropertyType) then
                                Seq.map (fun r -> writeRecord r mapper isInsert) (elem.GetValue(record, null) :?> seq<_>) 
                                |> Seq.toList
                            else
                                [writeRecord (elem.GetValue(record, null)) mapper isInsert])
            |> String.concat "\r\n go \r\n"

        let names,values = 
            scalar
            |> List.map (fun prop -> mapper.MapField prop, getSafeValue record prop)
            |> List.unzip

        sprintf "%s\r\n insert into %s (%s) values (%s)"
            vectorSql
            (mapper.MapRecord (record.GetType()))
            (String.concat ", " names)
            (String.concat ", " values)