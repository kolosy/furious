namespace Furious

module TableGraph =
    
    type table = {
        name: string
        alias: string
    }

    type union = {
        sourceColumn: string
        targetColumn: string
    }

    type vertex = table * (table * union) list

    let connect (source: vertex) (target: vertex) sourceColumn targetColumn =
        let t, u = source
        t, (fst target, { sourceColumn = sourceColumn; targetColumn = targetColumn }) :: u