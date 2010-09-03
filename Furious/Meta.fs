namespace Furious

module Meta =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Reflection

    open Join
    open Expression
    open Interfaces
    open RecordMapping
    open TypeUtils
    open ValueUtils

    type Datastore(conn: unit -> System.Data.Common.DbConnection, ?keyMapper:IRecordMapper, ?dialect:ISqlDialect) =
        let context = { mapper = match keyMapper with | Some m -> m | None -> new Mapper.DefaultRecordMapper() :> IRecordMapper
                        dialect = match dialect with | Some d -> d | None -> new Dialect.ANSISqlDialect() :> ISqlDialect }

        let prefix = "t1"

        member private x.RunSql sql =
            let command = conn().CreateCommand() 
            command.CommandText <- sql
            command.ExecuteReader()

        member x.Yield (expr: Expr<(seq<'a>->seq<'b>)>) = 
            let joins = computeJoins typeof<'b> context prefix
            let e,collation = traverse prefix joins context None expr
            let select = 
                match collation with 
                | Some e -> failwith (sprintf "unknown collation function %s" e)
                | None ->
                    computeSelectClause typeof<'a> prefix joins context true
            
            let from = computeFromClause typeof<'a> prefix context joins

            let sql = context.dialect.Select select from e
//            let sql = sprintf "select %s from %s %s" select from (if System.String.IsNullOrWhiteSpace e then emptyString else "where " + e)
            printfn "\r\n running sql \r\n%s" sql
            seq {
                use reader = x.RunSql sql

                // todo: this might advance twice
                while reader.Read() do
                    for r in (readRecord typeof<'a> prefix (context) reader None false) -> r :?> 'b
            }

        member x.Compute (expr: Expr<(seq<'a>->'b)>) = 
            let joins = computeJoins typeof<'a> context prefix
            let e,collation = traverse prefix joins context None expr
            let select = 
                match collation with 
                | Some "length" ->
                    context.dialect.DistinctCount prefix (context.mapper.GetPrimaryKeyName(typeof<'a>).Value)
//                    sprintf "count(distinct %s.%s)" prefix (context.mapper.GetPrimaryKeyName(typeof<'a>).Value)
                | Some e -> failwith (sprintf "unknown collation function %s" e)
                | None ->
                    computeSelectClause typeof<'a> prefix joins context true

            let from = computeFromClause typeof<'a> prefix context joins
            let sql = context.dialect.Select select from e
//            let sql = sprintf "select %s from %s %s" select from (if System.String.IsNullOrWhiteSpace e then emptyString else "where " + e)
            printfn "\r\n running sql \r\n%s" sql
            seq {
                use reader = x.RunSql sql

                while reader.Read() do
                    for r in (readRecord typeof<'b> prefix (context) reader None false) -> r :?> 'b
            }

        member x.Save record =
            let cmd = conn().CreateCommand()
            let sql = writeRecord context false record
            printfn "\r\n running sql \r\n%s" sql
            cmd.CommandText <- sql
            cmd.CommandType <- System.Data.CommandType.Text
            cmd.ExecuteNonQuery()