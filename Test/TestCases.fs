namespace Furious.Test

module TestCases =
    open Furious.Meta
    open NUnit.Framework

    type person = {
        personId: string
        firstname: string
        lastname: string
        homeAddress: address
        workAddress: address
        altAddresses: address seq
    }
    and address = {
        addressId: string
        street1: string
        zip: string
    }

    type personzip = {
        lastname: string
        zip: string
    }

    let areEqualSequences s1 s2 = 
        Set.ofSeq s1 - Set.ofSeq s2 = Set.empty

    let comparePeople (p1: person) (p2: person) = 
        (p1.personId = p2.personId) &&
        (p1.firstname = p2.firstname) &&
        (p1.lastname = p2.lastname) &&
        (p1.homeAddress = p2.homeAddress) &&
        (p1.workAddress = p2.workAddress) &&
        (p1.personId = p2.personId) &&
        (areEqualSequences p1.altAddresses p2.altAddresses)

    type GenerationTests() =
        let conn = new System.Data.SQLite.SQLiteConnection("Data Source=:memory:;Version=3;New=True")
        let db = Datastore(fun () -> upcast conn)

        let address1 = { addressId = System.Guid.NewGuid().ToString(); street1 = "1232 something"; zip = "60614" }
        let address2 = { addressId = System.Guid.NewGuid().ToString(); street1 = "456 something"; zip = "60614" }
        let address3 = { addressId = System.Guid.NewGuid().ToString(); street1 = "2344 something"; zip = "60614" }
        let address4 = { addressId = System.Guid.NewGuid().ToString(); street1 = "987 something"; zip = "60614" }

        //let newPerson = { personId = System.Guid.NewGuid().ToString(); firstname = "alex"; lastname = "pedenko"; homeAddress = address1; workAddress = address2; altAddresses = [address3; address4] }
        let newPerson = { personId = System.Guid.NewGuid().ToString(); firstname = "alex"; lastname = "pedenko"; homeAddress = address1; workAddress = address2; altAddresses = [ address3; address4 ] }
        
        [<Test>]
        member x.SimpleSelect() =
            let (neighbor: person seq) = db.Yield <@ Seq.filter (fun p -> p.homeAddress.zip = "60614") @>
            Assert.That(Seq.length neighbor = 1, sprintf "Expected 1 person, got %d" (Seq.length neighbor))
            Assert.That(comparePeople (Seq.head neighbor) newPerson)

        [<Test>]
        member x.ComputedValue() =
            let (neighbor: int seq) = db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>
            Assert.That(Seq.length neighbor = 1, sprintf "Expected 1 result, got %d" (Seq.length neighbor))
            Assert.AreEqual(1, Seq.head neighbor)

        [<Test>]
        member x.SimpleSelect2() =
            let (neighbor: person seq) = db.Yield <@ Seq.filter (fun p -> (p.homeAddress.zip = "60614") && (p.workAddress.zip = "60069") && ((p.firstname = "alex") || (p.lastname = "pedenko"))) @>
            Assert.That(Seq.length neighbor = 0, sprintf "Expected no results, got %d" (Seq.length neighbor))
            //Assert.AreEqual(1, Seq.head neighbor)

        [<Test>]
        member x.ComputedValue2() =
            let (neighbor: int seq) = db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> (p.homeAddress.zip = "60614") && (p.workAddress.zip = "60069") && ((p.firstname = "alex") || (p.lastname = "pedenko"))) people @>
            Assert.That(Seq.length neighbor = 1, sprintf "Expected 1 result, got %d" (Seq.length neighbor))
            Assert.AreEqual(0, Seq.head neighbor)

        [<TestFixtureSetUp>]
        member x.CreateSchemaAndUser() =
            conn.Open()
            let cmd = conn.CreateCommand()
            printfn "Creating tables..."
            cmd.CommandText <- "create table person (personId varchar primary key not null, firstname varchar, lastname varchar, homeAddressId varchar, workAddressId varchar)"
            cmd.ExecuteNonQuery() |> ignore
            cmd.CommandText <- "create table address (addressId varchar primary key not null, street1 varchar, zip varchar)"
            cmd.ExecuteNonQuery() |> ignore
            cmd.CommandText <- "create table personAltAddresses (addressId varchar not null, personId varchar not null)"
            cmd.ExecuteNonQuery() |> ignore

            db.Save newPerson |> ignore
//            
//        [<TestFixtureTearDown>]
//        member x.DropSchema() =
//            printfn "Droping tables..."
//            let cmd = conn.CreateCommand()
//            cmd.CommandText <- "drop table person"
//            cmd.ExecuteNonQuery() |> ignore
//            cmd.CommandText <- "drop table address"
//            cmd.ExecuteNonQuery() |> ignore
//            cmd.CommandText <- "drop table personAltAddresses"
//            cmd.ExecuteNonQuery() |> ignore
//            conn.Close()