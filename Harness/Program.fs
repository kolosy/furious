// Learn more about F# at http://fsharp.net

open Furious.Meta

type person = {
    firstname: string
    lastname: string
    homeAddress: address
    workAddress: address
    altAddresses: address seq
}
and address = {
    street1: string
    zip: string
}

type personzip = {
    lastname: string
    zip: string
}

let db = Datastore()
// simple filtering
// select firstname, lastname, homeAddressId from people p left outer join addresses a on p.homeAddressId = a.addressId where a.zip = '60614'
printfn "\r\nCase 1" |> ignore
let (neighbor: person seq) = db.Yield <@ fun people-> Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>

printfn "\r\nCase 2" |> ignore
let (neighbors: int seq) = db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>

printfn "\r\nCase 3" |> ignore
let (neighbor2: person seq) = db.Yield <@ Seq.filter (fun p -> (p.homeAddress.zip = "60614") && (p.workAddress.zip = "60069") && ((p.firstname = "alex") || (p.lastname = "pedenko"))) @>
    
printf "Done. Press any key to continue." |> ignore
System.Console.ReadLine() |> ignore