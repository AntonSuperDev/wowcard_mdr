import { Inject, Injectable } from "@nestjs/common";
import * as fs from "fs";
import csv from "csv-parser";
import { Pool } from "pg";

export type ProcessCustomerObject = {
    id: string;
    wcaid: string;
    software: string;
    wsid: string;
    shopname: string;
    authdate: Date;
    mbdayyr: string;
    mbdaymo: string;
    tbdayyr: string;
    firstname: string;
    lastname: string;
    address: string;
    city: string;
    state: string;
    zip: string;
    status: string;
    dpv: string;
    errorno: string;
    errornodesc: string;
    movedate: Date;
    nxi: string;
    dfl: string;
    dfldate: string;
    vacant: string;
    latitude: number;
    longitude: number;
    crrt: string;
    aptabrcd: string;
    aptunitcd: string;
    acoa: string;
    acoalevel: string;
    acoadate: null | Date;
    acoanxi: string;
    acoatype: string;
}

export type UnProcessCustomerObject = {
    id: number;
    shopid: string;
    firstname: string;
    lastname: string;
    email: string;
    address1: string;
    address2: string;
    city: string;
    state: string;
    zip: string;
    phone: string | null;
    customertype: string;
    okformarketing: boolean;
    birthday: Date | null;
    authdate: Date;
}

@Injectable()
export class MdrService {
    constructor(
        @Inject("DB_CONNECTION") private readonly db: Pool
    ) {}

    async spliteTekCustomers(tekShopId: number, wsId: string): Promise <
        {
            process: ProcessCustomerObject [],
            unprocess: UnProcessCustomerObject []
        }
    > {

        const tableID = Math.floor(tekShopId / 500);
        const res = await this.db.query(
            `
            WITH process AS (
                SELECT 
                    ac.id as id,
                    ac.wcaid as wcaid,
                    ac.software as software,
                    ac.wsid as wsid,
                    ac.shopname as shopname,
                    ac.authdate as authdate,
                    ac.mbdayyr as mbdayyr,
                    ac.mbdaymo as mbdaymo,
                    ac.tbdayyr as tbdayyr,
                    ac.firstname as firstname,
                    ac.lastname as lastname,
                    ac.address_ as address,
                    ac.city as city,
                    ac.state_ as state,
                    ac.zip as zip,
                    ac.status_ as status,
                    ac.dpv as dpv,
                    COALESCE(ac.errorno, '') AS errorno,
                    ac.errornodesc as errornodesc,
                    ac.movedate as movedate,
                    COALESCE(ac.nxi, '') AS nxi,
                    ac.dfl as dfl,
                    ac.dfldate as dfldate,
                    ac.vacant as vacant,
                    ac.latitude as latitude,
                    ac.longitude as longitude,
                    ac.crrt as crrt,
                    ac.aptabrcd as aptabrcd,
                    ac.aptunitcd as aptunitcd,
                    ac.acoa as acoa,
                    ac.acoalevel as acoalevel,
                    ac.acoadate as acoadate,
                    ac.acoanxi as acoanxi,
                    ac.acoatype as acoatype
                FROM accuzipcustomertb ac
                INNER JOIN tekcustomer${tableID} c ON CAST(c.id AS text) = ac.id
                WHERE ac.wsid = '${wsId}'
                AND c.shopid = '${tekShopId}'
            ),
            unprocess AS (
                SELECT
                    c.id as id,
                    c.shopid as shopid,
                    COALESCE(c.firstname, '') AS firstname,
                    COALESCE(c.lastname, '') AS lastname,
                    COALESCE(c.address1, '') AS address1,
                    COALESCE(c.address2, '') AS address2,
                    c.email as email,
                    c.address_city as city,
                    c.address_state as state,
                    c.address_zip as zip,
                    c.phone1 as phone,
                    c.customertype_code as customertype,
                    c.okformarketing as okformarketing,
                    c.birthday as birthday,
                    MAX(j.authorizeddate) as authdate
                FROM tekcustomer${tableID} c
                LEFT JOIN tekjob${tableID} j ON CAST(j.customerid AS text) = CAST(c.id AS text)
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM accuzipcustomertb ac
                    WHERE CAST(c.id AS text) = ac.id AND ac.wsid = '${wsId}'
                    AND c.shopid = '${tekShopId}'
                )
                AND c.shopid = '${tekShopId}'
                GROUP BY c.id, c.shopid, c.firstname, c.lastname, c.email, c.address1, c.address2, c.address_city, c.address_state, c.address_zip, c.phone1, c.customertype_code, c.okformarketing, c.birthday
            )
            SELECT json_build_object(
                'process', (SELECT json_agg(p) FROM process p),
                'unprocess', (SELECT json_agg(u) FROM unprocess u)
            ) as result;
            `
        )

        return res.rows[0].result;
    }
}