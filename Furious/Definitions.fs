namespace Furious

module Definitions =

    open System
    open System.Reflection

    type direction =
    | Inner
    | Left
    | Right

    type join = {
        table: string
        alias: string
        fkName: string
        pkName: string
        direction: direction
        selectable: bool
        definingMember: PropertyInfo
    }

    let emptyString = System.String.Empty