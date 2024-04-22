import { Inject, Injectable } from "@nestjs/common";
import * as fs from "fs";
import csv from "csv-parser";
import { Pool } from "pg";

import { ProcessCustomerObject } from "./mdr.service";

@Injectable()
export class MdrIdentifyValideService {
    constructor(
        @Inject("DB_CONNECTION") private readonly db: Pool
    ) {}

    private static readonly earthRadiusInMiles = 3959;
    
    private toRadians(degrees: number): number {
        return (degrees * Math.PI) / 180;
    };

    calculateDistance(lat1: number, lon1: number, lat2: number, lon2: number) {
        const dLat = this.toRadians(lat2 - lat1);
        const dLon = this.toRadians(lon2 - lon1);
    
        const a =
          Math.sin(dLat / 2) * Math.sin(dLat / 2) +
          Math.cos(this.toRadians(lat1)) *
            Math.cos(this.toRadians(lat2)) *
            Math.sin(dLon / 2) *
            Math.sin(dLon / 2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    
        const distance = MdrIdentifyValideService.earthRadiusInMiles * c;
        return distance;
    };

    async addValidFlag(processCustomers: ProcessCustomerObject [], shopLat: number, shopLon: number, limiteMiles: number) {
        const date48Ago = new Date();
        date48Ago.setMonth(date48Ago.getMonth() - 48);
        const date36Ago = new Date();
        date36Ago.setMonth(date36Ago.getMonth() - 36);
        const date24Ago = new Date();
        date24Ago.setMonth(date24Ago.getMonth() - 24);
        const date12Ago = new Date();
        date12Ago.setMonth(date12Ago.getMonth() - 12);

        // Count of specific columns of MDR
        let countU12Valid : number = 0;
        let countU12ValidWithBday : number = 0;
        let countU24Valid: number = 0;
        let countU24ValidWithBday : number = 0;
        let countU36Valid: number = 0;
        let countU36ValidWithBday : number = 0;
        let countU48Valid: number = 0;
        let countU48ValidWithBday : number = 0;
        let countU48AllValid: number = 0;
        let countU48AllValidWithBday : number = 0;
        let countO48AllValid: number = 0;
        let countO48AllValidWithBday : number = 0;
        let countTooFarU48Valid: number = 0;
        let countTooFarO48Valid: number = 0;
        let countU48AllInvalid: number = 0;
        let countO48AllInvalid: number = 0;
        let countU48BadUnit: number = 0;
        let countU48NewAdd: number = 0;
        let countO48NewAdd: number = 0;


        const limitedCustomers = processCustomers.map(customer => {
            // Calculate the distance between the customer and shop and add valid falg;
            const customerLat = customer.latitude;
            const customerLong = customer.longitude;
            const authDate = new Date(customer.authdate);
            let distance = this.calculateDistance(customerLat, customerLong, shopLat, shopLon);
            // Identify if the customers are missing Unit number or not;
            const errNo = customer.errorno;
            
            const isMissingUnitNo = [`12.2`, `12.3`].some(element => errNo.includes(element));

            // Identify if the vacant value of customers are 'Y';
            const vacant = customer.vacant;
            // Identify if the address of customers are changed;
            const nxiCode = customer.nxi;

            let isU12Valid = false;
            let isU12ValidWithBday = false;
            let isU24Valid = false;
            let isU24ValidWithBday = false;
            let isU36Valid = false;
            let isU36ValidWithBday = false;
            let isU48Valid = false;
            let isU48ValidWithBday = false;
            let isU48AllValid = false;
            let isU48AllValidWithBday = false;
            let isO48AllValid = false;
            let isO48AllValidWithBday = false;
            let isO48AllValidNeedBday = false;
            let isU48AllInvalid = false;
            let isO48AllInvalid = false;
            let isTooFarU48Valid = false;
            let isTooFarO48Valid = false;
            let isU48NewAdd = false;
            let isO48NewAdd = false;
            let isU48BadUnit = false;

            if (distance <= limiteMiles && vacant !== 'Y' && nxiCode != '02' && customer.status === 'V') {

                if (authDate >= date12Ago) {
                    isU12Valid = true;
                    isU48AllValid = true;
                    countU12Valid ++;
                    countU48AllValid ++ ;
                    if (Number(customer.mbdaymo) !== 0 ) {
                        isU12ValidWithBday = true;
                        isU48AllValidWithBday = false;
                        countU12ValidWithBday ++;
                        countU48AllValidWithBday ++ ;
                    }
                }

                if (authDate < date12Ago && authDate >= date24Ago) {
                    isU24Valid = true;
                    isU48AllValid = true;
                    countU24Valid ++;
                    countU48AllValid ++;
                    if (Number(customer.mbdaymo) !== 0 ) {
                        isU24ValidWithBday = true;
                        isU48AllValidWithBday = false;
                        countU24ValidWithBday ++;
                        countU48AllValidWithBday ++;
                    }
                }

                if (authDate < date24Ago && authDate >= date36Ago) {
                    isU36Valid = true;
                    isU48AllValid = true;
                    countU36Valid ++;
                    countU48AllValid ++;
                    if (Number(customer.mbdaymo) !== 0 ) {
                        isU36ValidWithBday = true;
                        isU48AllValidWithBday = false;
                        countU36ValidWithBday ++;
                        countU48AllValidWithBday ++;
                    }
                }

                if (authDate < date36Ago && authDate >= date48Ago) {
                    isU48Valid = true;
                    isU48AllValid = true;
                    countU48Valid ++;
                    countU48AllValid ++;
                    if (Number(customer.mbdaymo) !== 0 ) {
                        isU48ValidWithBday = true;
                        isU48AllValidWithBday = false;
                        countU48ValidWithBday ++;
                        countU48AllValidWithBday ++;
                    }
                }

                if (authDate < date48Ago) {
                    isO48AllValid = true;
                    countO48AllValid ++;
                    if(Number(customer.mbdaymo) !== 0) {
                        isO48AllValidWithBday = true;
                        countO48AllValidWithBday ++;
                    }
                        
                }

            } else {
                if (authDate >= date48Ago) {
                    isU48AllInvalid = true;
                    countU48AllInvalid ++;
                } else {
                    isO48AllInvalid = true;
                    countO48AllInvalid ++;
                }
            }

            if (distance > limiteMiles && vacant !== 'Y' && nxiCode != '02' && customer.status === 'V') {
                if (authDate >= date48Ago) {
                    isTooFarU48Valid = true;
                    countTooFarU48Valid ++;
                } else {
                    isTooFarO48Valid = true;
                    countTooFarO48Valid ++;
                }
            }

            if ((nxiCode === 'A' || nxiCode === '91' || nxiCode === '92') && customer.status === 'V' && customer.vacant != 'Y') {
                if (authDate < date48Ago) {
                    isO48NewAdd = true;
                    countO48NewAdd ++;
                } else {
                    isU48NewAdd = true;
                    countU48NewAdd ++;
                }
            }

            if (authDate >= date48Ago && (errNo.includes('12.2') || errNo.includes('12.3') || errNo.includes('12.4'))) {
                isU48BadUnit = true;
                countU48BadUnit;
            }
    
            return {
                ...customer,
                isTooFar: distance >= limiteMiles,
                isMissingUnitNo,
                isVacant: vacant !== 'Y',
                isAddressMovedNew: nxiCode !== 'A',
                isAddressMovedNG: nxiCode !== '02',
                isU12Valid,
                isU24Valid,
                isU36Valid,
                isU48Valid,
                isU48AllValid,
                isO48AllValid,
                isU48AllInvalid,
                isO48AllInvalid,
                isTooFarU48Valid,
                isTooFarO48Valid,
                isU12ValidWithBday,
                isU24ValidWithBday,
                isU36ValidWithBday,
                isU48ValidWithBday,
                isU48AllValidWithBday,
                isO48AllValidWithBday,
                isO48NewAdd,
                isU48NewAdd,
                isU48BadUnit
            };
        });
    
        return {
            customers: limitedCustomers,
            count: {
                countU12Valid ,
                countU12ValidWithBday ,
                countU24Valid,
                countU24ValidWithBday ,
                countU36Valid,
                countU36ValidWithBday ,
                countU48Valid,
                countU48ValidWithBday ,
                countU48AllValid,
                countU48AllValidWithBday ,
                countO48AllValid,
                countO48AllValidWithBday ,
                countTooFarU48Valid,
                countTooFarO48Valid,
                countU48AllInvalid,
                countO48AllInvalid,
                countU48BadUnit,
                countU48NewAdd,
                countO48NewAdd,
            }
        };
    }
}