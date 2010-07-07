namespace Furious

module RecordMapping =
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Collections

    open System.Reflection
    open System.Data

    open Interfaces
    open TypeUtils
    open ValueUtils

//                    computeFieldNames typeof<'b> (fst newUnions.[typeof<'b>.Name]).alias x.Mapper

    let computeFieldNames recType alias (mapper: IRecordMapper) =
        FSharpType.GetRecordFields (recType)
        |> Array.map (fun e -> sprintf "%s.%s" alias (mapper.MapField e))
        |> String.concat ", "

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