// Learn more about F# at http://fsharp.net

open Furious.Meta

type person = {
    firstname: string
    lastname: string
    homeAddress: address
    workAddress: address option
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
let (neighbor2: person seq) = db.Yield <@ Seq.filter (fun p -> (p.homeAddress.zip = "60614") && (p.workAddress.Value.zip = "60069") && ((p.firstname = "alex") || (p.lastname = "pedenko"))) @>
    
let address1 = { street1 = "1232 something"; zip = "60614" }
let address2 = { street1 = "456 something"; zip = "60614" }
let address3 = { street1 = "2344 something"; zip = "60614" }
let address4 = { street1 = "987 something"; zip = "60614" }

let newPerson = { firstname = "alex"; lastname = "pedenko"; homeAddress = address1; workAddress = Some address2; altAddresses = [address3; address4] }

printf "\r\n Case 4"
printf "%s" (db.Save newPerson)

printf "Done. Press any key to continue." |> ignore
System.Console.ReadLine() |> ignore

