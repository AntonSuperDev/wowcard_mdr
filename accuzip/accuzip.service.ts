import { Inject, Injectable } from "@nestjs/common";
import * as fs from "fs";
import csv from "csv-parser";
import { Pool } from "pg";
import { AccuzipCalculateDistanceService } from "./accuzip.calculatedistance.service";

type AccuzipCustomerObject = {
    id: string;
    wcaId: string;
    software: string;
    shopId: string;
    wsId: string;
    shopName: string;
    authDate: Date;
    strAuthDate: string;
    mbdayyr: string;
    mbdaymo: string;
    tbdaymo: string;
    firstName: string;
    lastName: string;
    address: string;
    city: string;
    state: string;
    zip: string;
    status: string;
    dpv: string;
    errorNo: string [];
    errorNoDesc: string;
    moveDate: Date | null;
    nxi: string;
    dfl: string;
    dflDate: string;
    vacant: string;
    latitude: number;
    longitude: number;
    crrt: string;
    aptBrcd: string;
    aptUnitCD: string;
    acoa: string;
    acoaLevel: string;
    acoaDate: string;
    acoaNxi: string;
    acoaType: string;
}

type CountDetails = {
    wsId: string;
    software: string;
    shopname: string;
    totalBdaymo: number;
    u48bdaymo: number [];
    o48bdaymo: number [];
    totalTdaymo: number;
    u48tdaymo: number [];
    o48tdaymo: number [];
    u48validBday: number;
    u48validNoBday: number;
    o48validBday: number;
    o48validNoBday: number;
}

type CountMapObject = {
    [shopname: string]: CountDetails;
}

@Injectable()
export class AccuzipService {
    constructor(
        private readonly accuzipCalculateDistanceService: AccuzipCalculateDistanceService,
        @Inject("DB_CONNECTION") private readonly db: Pool
    ) {}

    // Read raw the Customers who were processed by enhanced Accuzip APi.
    // Remove the invalid Customers based on vacant = 'Y', Status_ != 'V' and nxi != '2'

    async readAccuzipCustomers(): Promise<AccuzipCustomerObject []> {
        const res = await this.db.query(
            `
            SELECT * FROM accuzipcustomertb
            `
        )
        return res.rows.reduce((tot, customer) => (customer.vacant !== 'Y' && customer.status_ === 'V' && customer.nxi !== '2') ? [...tot, {
            id: customer.id,
            wcaId: customer.wcaid,
            software: customer.software,
            shopId: customer.shopid,
            wsId: customer.wsid,
            shopName: customer.shopname,
            strAuthDate: customer.authdate.toISOString().split("T")[0],
            authDate: customer.authdate,
            mbdayyr: customer.mbdayyr,
            mbdaymo: customer.mbdaymo,
            tbdayyr: customer.tbdayyr,
            firstName: customer.firstname,
            lastName: customer.lastname,
            address: customer.address_,
            city: customer.city,
            state: customer.state_,
            zip: customer.zip,
            status: customer.status_,
            dpv: customer.dpv,
            errorNo: customer.errorno.split(","),
            errorNoDesc: customer.errornodesc,
            moveDate: customer.movedate,
            nxi: customer.nxi,
            dfl: customer.dfl,
            dflDate: customer.dfldate,
            vacant: customer.vacant,
            latitude: customer.latitude,
            longitude: customer.longitude,
            crrt: customer.crrt,
            aptBrcd: customer.aptabrcd,
            aptUnitCD: customer.aptunitcd,
            acoa: customer.acoa,
            acoaLevel: customer.acoalevel,
            acoaDate: customer.acoadate,
            acoaNxi: customer.acoanxi,
            acoaType: customer.acoatype 
        }]: tot, [])
    }

    // Deduplicated the Customers based on same firstname, lastname, address as well as chainId and wsid.
    // Limited the Customers based on authDate

    async deduplicate(dateAgo: number, isLimitedAuthdate: boolean, isUnder: boolean): Promise<AccuzipCustomerObject []> {
        const validAccuzipCustomers = await this.readAccuzipCustomers();
        const uniqueCustomersMap = new Map<string, AccuzipCustomerObject>();
        const sortedCustomers = validAccuzipCustomers.sort((a,b) => b.authDate?.getTime() - a.authDate?.getTime());
        sortedCustomers.forEach(customer => {
            const {
                firstName,
                lastName,
                address,
                wcaId,
                wsId,
                authDate
            } = customer;

            const key = wcaId !== '0'
                ? `${firstName}-${lastName}-${address}-${wcaId}`
                : `${firstName}-${lastName}-${address}-${wsId}`;
            
            if (!uniqueCustomersMap.has(key) || (uniqueCustomersMap.get(key)?.authDate ?? new Date('') < authDate)) {
                if (uniqueCustomersMap.has(key)) {
                    console.log(uniqueCustomersMap.get(key))
                }
                uniqueCustomersMap.set(key, customer);
            }
        });

        const agoDate = new Date();
        agoDate.setMonth(agoDate.getMonth() - dateAgo);

        return !isLimitedAuthdate
                    ? Array.from(uniqueCustomersMap.values())
                    : isUnder ? Array.from(uniqueCustomersMap.values()).filter(customer => customer.authDate > agoDate)
                              : Array.from(uniqueCustomersMap.values()).filter(customer => customer.authDate <= agoDate);
    }

    // Limit the Customers based on miles per shop

    async limitMiles(dateAgo: number, isLimitedAuthdate: boolean, isUnder: boolean) {
        const deduplicatedCustomers = await this.deduplicate(dateAgo, isLimitedAuthdate, isUnder);
        const shopsInfor = await this.db.query(`SELECT w.id as id, w.limited_miles as miles, w.latitude_ as latitude, w.longitude_ as longitude FROM wowcardshoptb AS w`);

        console.log(shopsInfor.rows);
        const limitedCustomers = await Promise.all(
            deduplicatedCustomers.reduce((tot: AccuzipCustomerObject[], customer: AccuzipCustomerObject) => {
                const customerLat = customer.latitude;
                const customerLon = customer.longitude;
                const shopId = customer.wsId;
                const shopLat = shopsInfor.rows.find(shop => shop.id === shopId).latitude;
                const shopLon = shopsInfor.rows.find(shop => shop.id === shopId).longitude;
                const shopMiles = shopsInfor.rows.find(shop => shop.id === shopId).miles;
                let distance = this.accuzipCalculateDistanceService.calculateDistance(customerLat, customerLon, shopLat, shopLon);

                return distance <= shopMiles ? [...tot, customer] : tot;
            }, [])
        )

        return limitedCustomers;
    }

    // Assign the Tday to Customers that don't have valid Bdays.

    async assignTday(dateAgo: number): Promise <AccuzipCustomerObject [][]> {
        let agoDate = new Date();
        agoDate.setMonth(agoDate.getMonth() - dateAgo);
        
        const limitedCustomers = await this.limitMiles(48, false, true);
        let u48validCustomers: AccuzipCustomerObject [] = [];
        let o48validCustomers: AccuzipCustomerObject [] = [];

        limitedCustomers.forEach(customer => new Date(customer.authDate) > agoDate ? u48validCustomers.push(customer) : o48validCustomers.push(customer));

        return [u48validCustomers, o48validCustomers].map(customers => {
            const sortedCustomers = customers.sort((a: AccuzipCustomerObject , b: AccuzipCustomerObject) => {
                const dateComparison = new Date(a.authDate).getTime() - new Date(b.authDate).getTime();

                if (dateComparison !== 0) return dateComparison;

                return Number(a.mbdaymo) - Number(b.mbdaymo);
            })

            const countsByShopAndMonth: Record <string, Record<string, number>> = {};
            
            sortedCustomers.forEach(customer => {
                let { wsId, mbdaymo } = customer;

                mbdaymo = Number(mbdaymo) ? Number(mbdaymo).toString(): '0';

                if(!(wsId in countsByShopAndMonth)) {
                    countsByShopAndMonth[wsId] = {};
                }

                countsByShopAndMonth[wsId][mbdaymo] = (countsByShopAndMonth[wsId][mbdaymo] || 0) + 1;
            });

            const averageByShop: Record<string, {average: number; remainder: number}> = {};

            for (const wsId in countsByShopAndMonth) {
                const counts = Object.values(countsByShopAndMonth[wsId]);
                const totalCustomers = counts.reduce((total, count) => total + count, 0);
                const average = Math.floor(totalCustomers / 12);
                const remainder = totalCustomers % 12;
                averageByShop[wsId] = { average, remainder };
            }

            const tbdayCounts: Record <string, Record <string, number>> = {};
            for (const wsId in countsByShopAndMonth) {
                tbdayCounts[wsId] = {};
                for (const mbdaymo in countsByShopAndMonth[wsId]) {
                  if (mbdaymo !== "0") {
                    const actualCount = countsByShopAndMonth[wsId][mbdaymo];
                    let { average, remainder } = averageByShop[wsId];
                    tbdayCounts[wsId][mbdaymo] = Math.max(0, average - actualCount);
                  }
                }
        
                let tbdaySum = Object.values(tbdayCounts[wsId]).reduce((sum, count) => sum + count, 0);
                averageByShop[wsId].remainder = countsByShopAndMonth[wsId]["0"] - tbdaySum;

                while (averageByShop[wsId].remainder < 0) {
                    let { average, remainder } = averageByShop[wsId];
                    average -= 1;
                    for (const mbdaymo in countsByShopAndMonth[wsId]) {
                      if (mbdaymo !== "0") {
                        const actualCount = countsByShopAndMonth[wsId][mbdaymo];
                        tbdayCounts[wsId][mbdaymo] = Math.max(0, average - actualCount);
                      }
                    }
          
                    let tbdaySum = Object.values(tbdayCounts[wsId]).reduce(
                      (sum, count) => sum + count,
                      0,
                    );
                    averageByShop[wsId].remainder =
                      countsByShopAndMonth[wsId]["0"] - tbdaySum;
                    averageByShop[wsId].average = average;
                }
            }

            for (const wsId in averageByShop) {
                const { average, remainder } = averageByShop[wsId];
                const months = Object.keys(countsByShopAndMonth[wsId]).slice(1);
                    if (remainder >= 0) {
                        months.sort((a, b) =>countsByShopAndMonth[wsId][a] - countsByShopAndMonth[wsId][b],
                    );

                    const lowestCountMonth = months.find((month) => countsByShopAndMonth[wsId][month] > 0);

                    if (lowestCountMonth) {
                        tbdayCounts[wsId][lowestCountMonth] += remainder;
                    }
                }
            }

            const results: Record<string, any>[] = [];

            for (const wsId in countsByShopAndMonth) {
                for (const mbdaymo in countsByShopAndMonth[wsId]) {
                if (mbdaymo !== "0") {
                        const actualCount = countsByShopAndMonth[wsId][mbdaymo] || 0;
                        const tbdayCount = tbdayCounts[wsId][mbdaymo] || 0; // Ensuring tbdayCount is defined and falls back to 0 if undefined
                        results.push({
                        id: wsId,
                        mbdaymo,
                        bdaymo: actualCount,
                        tbdaymo: tbdayCount,
                        all: actualCount + tbdayCount,
                        });
                    }
                }
            }

            const groupData: { [key: string]: any[] } = {};

            results.forEach((data) => {
                if (!groupData[data.id]) {
                    groupData[data.id] = [];
                }
                groupData[data.id].push(data);
            });

            for (const id in groupData) {
                groupData[id].sort((a, b) => a.bdaymo - b.bdaymo);
            }

            const customersWithEmptyMbdaymo = sortedCustomers.sort(
                (a, b) =>
                new Date(b.authDate).getTime() - new Date(a.authDate).getTime(),
            );

            let customerUpdateCount: { [key: string]: number } = {};
            Object.keys(groupData).forEach((id) => {
                const shopData = groupData[id];
                shopData.forEach((monthData) => {
                    if (!customerUpdateCount[monthData.mbdaymo]) {
                        customerUpdateCount = {
                            ...customerUpdateCount,
                            [monthData.mbdaymo]: 0,
                        };
                    }
                    for (let i = 0; i < customersWithEmptyMbdaymo.length; i++) {
                        let customer = customersWithEmptyMbdaymo[i];
                        if (
                            customer.wsId.trim() === id.trim() &&
                            Number(customer.mbdaymo) ? Number(customer.mbdaymo) : 0 === 0 &&
                            Number(customer.tbdaymo) ? Number(customer.tbdaymo) : 0 === 0
                        )
                        {
                            customer.tbdaymo = monthData.mbdaymo;
                            customerUpdateCount = {
                                ...customerUpdateCount,
                                [monthData.mbdaymo]: customerUpdateCount[monthData.mbdaymo] + 1,
                            };
                        }
                        if (customerUpdateCount[monthData.mbdaymo] >= monthData.tbdaymo) {
                            customerUpdateCount = {
                                ...customerUpdateCount,
                                [monthData.mbdaymo]: 0,
                            };
                            break;
                        }
                    }
                });
            });
        
            return customersWithEmptyMbdaymo;
        })
    }

    // Limit the count of Customers based on annual account per shop.

    async limitBasedonAnnualCountsFocusAuthDate(dateAgo: number) {
        const tdayAssignedCustomers = await this.assignTday(dateAgo);
        const sortedCustomers = tdayAssignedCustomers.flat().sort((a, b) => {
            const wsIDDiff = parseInt(a.wsId) - parseInt(b.wsId);
            if (wsIDDiff !==  0) {
                return wsIDDiff;
            } else {
                const dateDiff = new Date(a.authDate).getTime() - new Date(b.authDate).getTime();
                if (dateDiff !== 0) {
                    return dateDiff;
                } else {
                    const mbdaymoDiff = parseInt(b.mbdaymo) - parseInt(a.mbdaymo);
                    return mbdaymoDiff;
                }
            }
        });

        const annualCounts = await this.db.query(`SELECT w.id as id, w.contract_count as count FROM wowcardshoptb AS w`);
        const counts: CountMapObject = {};
        let agoDate = new Date();
        agoDate.setMonth(agoDate.getMonth() - 48);
        for (const item of sortedCustomers) {
            if (!counts[item.wsId]) {
                counts[item.wsId] = {
                    wsId: item.wsId,
                    software: item.software,
                    shopname: item.shopName,
                    totalBdaymo: 0,
                    u48bdaymo: Array(12).fill(0),
                    o48bdaymo: Array(12).fill(0),
                    totalTdaymo: 0,
                    u48tdaymo: Array(12).fill(0),
                    o48tdaymo: Array(12).fill(0),
                    u48validBday: 0,
                    u48validNoBday: 0,
                    o48validBday: 0,
                    o48validNoBday: 0,
                };
            }

            if (Number(item.mbdaymo) !== 0) {
                counts[item.wsId].totalBdaymo++;
                if (new Date(item.authDate) >= agoDate) {
                    counts[item.wsId].u48bdaymo[parseInt(item.mbdaymo) - 1]++;
                    counts[item.wsId].u48validBday++;
                } else {
                    counts[item.wsId].o48validBday++;
                    counts[item.wsId].o48bdaymo[parseInt(item.mbdaymo) - 1]++;
                }
            }
        
              if (Number(item.tbdaymo) !== 0) {
                counts[item.wsId].totalTdaymo++;
                if (new Date(item.authDate) >= agoDate) {
                    counts[item.wsId].u48tdaymo[parseInt(item.tbdaymo) - 1]++;
                    counts[item.wsId].u48validNoBday++;
                } else {
                    counts[item.wsId].o48validNoBday++;
                    counts[item.wsId].o48tdaymo[parseInt(item.tbdaymo) - 1]++;
                }
            }
        }

        let limitedCustomers: AccuzipCustomerObject[][] = [];

        for (const wsId in counts) {

            let shopData = counts[wsId];
            const contractCounts = annualCounts.rows.filter(
                (item) => item.id === wsId,
            );

            const bdayCustomersForShop = sortedCustomers.filter(
                (c) => c.wsId === wsId && Number(c.mbdaymo) !== 0,
            );

            const tdayCustomersForShop = sortedCustomers.filter(
                (c) => c.wsId === wsId && Number(c.tbdaymo) !== 0,
            );

            if (contractCounts[0].contract === 0) {
                contractCounts[0].deltaValue = 0;
            } else {
                contractCounts[0].deltaValue =
                shopData.totalBdaymo +
                shopData.totalTdaymo -
                contractCounts[0].contract;
            }

            if (contractCounts[0].deltaValue <= 0) {
                limitedCustomers.push(bdayCustomersForShop);
                limitedCustomers.push(tdayCustomersForShop);

                continue;
            }

            if (
                0 < contractCounts[0].contract &&
                contractCounts[0].contract <= shopData.u48validBday
            ) {
                let deductedCount = shopData.u48validBday - contractCounts[0].contract;
                shopData.u48tdaymo.fill(0);
                shopData.o48bdaymo.fill(0);
                shopData.o48tdaymo.fill(0);
                while (1) {
                    let deductedForMonth =
                        deductedCount >= 12 ? Math.floor(deductedCount / 12) : 1;
                    for (let i = 0; i < 12; i++) {
                        if (deductedCount <= 0) {
                        break;
                        }

                        if (shopData.u48bdaymo[i] > deductedForMonth) {
                        shopData.u48bdaymo[i] = shopData.u48bdaymo[i] - deductedForMonth;
                        deductedCount = deductedCount - deductedForMonth;
                        } else {
                        deductedCount = deductedCount - shopData.u48bdaymo[i];
                        shopData.u48bdaymo[i] = 0;
                        }
                    }

                    if (deductedCount <= 0) {
                        break;
                    }
                }
            } else if (
                contractCounts[0].contract > shopData.u48validBday &&
                contractCounts[0].contract <=
                shopData.u48validBday + shopData.u48validNoBday
            ) {
                shopData.o48bdaymo.fill(0);
                shopData.o48tdaymo.fill(0);
                let deductedCount =
                shopData.u48validBday +
                shopData.u48validNoBday -
                contractCounts[0].contract;
                let freq = Array(12).fill(0);
                for (let i = 0; i < 12; i++) {
                    freq[i] = shopData.u48tdaymo[i] / shopData.u48validNoBday;
                }
                while (1) {
                    for (let i = 0; i < 12; i++) {
                        if (deductedCount <= 0) {
                        break;
                        }

                        let deductedForMonth = Math.floor(deductedCount * freq[i]) + 1;
                        if (shopData.u48tdaymo[i] > deductedForMonth) {
                        shopData.u48tdaymo[i] = shopData.u48tdaymo[i] - deductedForMonth;
                        deductedCount = deductedCount - deductedForMonth;
                        } else {
                        deductedCount = deductedCount - shopData.u48tdaymo[i];
                        shopData.u48tdaymo[i] = 0;
                        }
                    }

                    if (deductedCount <= 0) {
                        break;
                    }
                }
            } else if (
                contractCounts[0].contract <=
                shopData.u48validBday +
                    shopData.u48validNoBday +
                    shopData.o48validBday &&
                contractCounts[0].contract >
                shopData.u48validBday + shopData.u48validNoBday
            ) {
                let deductedCount =
                shopData.u48validBday +
                shopData.u48validNoBday +
                shopData.o48validBday -
                contractCounts[0].contract;
                shopData.o48tdaymo.fill(0);
                while (1) {
                    let deductedForMonth =
                        deductedCount >= 12 ? Math.floor(deductedCount / 12) : 1;
                    for (let i = 0; i < 12; i++) {
                        if (deductedCount <= 0) {
                        break;
                        }

                        if (shopData.o48bdaymo[i] > deductedForMonth) {
                        shopData.o48bdaymo[i] = shopData.o48bdaymo[i] - deductedForMonth;
                        deductedCount = deductedCount - deductedForMonth;
                        } else {
                        deductedCount = deductedCount - shopData.o48bdaymo[i];
                        shopData.o48bdaymo[i] = 0;
                        }
                    }

                    if (deductedCount <= 0) {
                        break;
                    }
                }
            } else if (
                contractCounts[0].contract >
                shopData.u48validBday +
                    shopData.u48validNoBday +
                    shopData.o48validBday &&
                contractCounts[0].contract <=
                shopData.u48validBday +
                    shopData.u48validNoBday +
                    shopData.o48validBday +
                    shopData.o48validNoBday
            ) {
                let deductedCount =
                shopData.u48validBday +
                shopData.u48validNoBday +
                shopData.o48validBday +
                shopData.o48validNoBday -
                contractCounts[0].contract;
                let freq = Array(12).fill(0);
                for (let i = 0; i < 12; i++) {
                    freq[i] = shopData.o48tdaymo[i] / shopData.o48validNoBday;
                }
                while (1) {
                    for (let i = 0; i < 12; i++) {
                        if (deductedCount <= 0) {
                        break;
                        }

                        let deductedForMonth = Math.floor(deductedCount * freq[i]) + 1;
                        if (shopData.o48tdaymo[i] > deductedForMonth) {
                        shopData.o48tdaymo[i] = shopData.o48tdaymo[i] - deductedForMonth;
                        deductedCount = deductedCount - deductedForMonth;
                        } else {
                        deductedCount = deductedCount - shopData.o48tdaymo[i];
                        shopData.o48tdaymo[i] = 0;
                        }
                    }

                    if (deductedCount <= 0) {
                        break;
                    }
                }
            }

            for (let month = 1; month <= 12; month++) {
                const bdayCustomerForMonth = bdayCustomersForShop.filter(
                    (customer) => Number(customer.mbdaymo) === month,
                );
                const tdayCustomerForMonth = tdayCustomersForShop.filter(
                    (customer) => Number(customer.tbdaymo) === month,
                );
                limitedCustomers.push(
                    bdayCustomerForMonth.slice(
                        bdayCustomerForMonth.length -
                        (shopData.u48bdaymo[month - 1] + shopData.o48bdaymo[month - 1]),
                    ),
                );
                limitedCustomers.push(
                    tdayCustomerForMonth.slice(
                        tdayCustomerForMonth.length -
                        (shopData.u48tdaymo[month - 1] + shopData.o48tdaymo[month - 1]),
                    ),
                );
            }
        }

        return limitedCustomers.flat();

    }

    // Generate the mailing Lists based on specific month

    async generateMailingLists(month: number, dateAgo: number) {
        const customers = await this.limitBasedonAnnualCountsFocusAuthDate(dateAgo);
        const customersWithSpeificMonth = customers.filter(
            customer => 
                Number(customer.mbdaymo) === month ||
                Number(customer.mbdaymo) === month - (month < 7 ? -6 : 6) ||
                Number(customer.tbdaymo) === month - (month < 7 ? -6 : 6) ||
                Number(customer.tbdaymo) === month
        ).sort((a, b) => {
            const wsIDDiff = Number(a.wsId) - Number(b.wsId);

            if (wsIDDiff !== 0) {
                return wsIDDiff;
            } else {
                const mbdaymoDiff = Number(b.mbdaymo) - Number(a.mbdaymo);

                if (mbdaymoDiff !== 0) {
                    return mbdaymoDiff;
                } else {
                    const dateDiff =
                    new Date(b.authDate).getTime() - new Date(a.authDate).getTime();
                    return dateDiff;
                }
            }
        });

        const combinedArray = ([] as AccuzipCustomerObject []).concat(...Object.values(customersWithSpeificMonth));

        const updatedCustomers = combinedArray.map(customer => {
            if (Number(customer.mbdaymo) === month - (month < 7 ? -6 : 6)) {
                return {
                  ...customer,
                  ListName: `HDayList ${month}`,
                };
            } else if (Number(customer.mbdaymo) === month) {
                return {
                  ...customer,
                  ListName: `BDayList ${month}`,
                };
            } else if (
                Number(customer.tbdaymo) === month ||
                Number(customer.tbdaymo) === month - (month < 7 ? -6 : 6)
            ) {
                return {
                  ...customer,
                  ListName: `THDayList ${month}`,
                };
            } else {
                return {
                  ...customer,
                  ListName: "",
                };
            }
        })

        return updatedCustomers;
    }

    // Generate the mailing Lists based on specific month per shop

    async generateMailingListsPerShop(month: number, date48Ago: number) {
        const customers = await this.generateMailingLists(month, date48Ago);
        const wsIdListName = new Map<string, (AccuzipCustomerObject & {ListName: string})[]>();
        const keys = new Map<string, number>();
        customers.forEach(customer => {
            const {wsId, ListName, shopName }= customer;

            if (ListName != "") {
                const key = `${wsId}-${ListName}-${shopName}`;
                keys.set(key, (keys.get(key) || 0) + 1);
                if (!wsIdListName.has(key)) {
                    wsIdListName.set(key, []);
                }
                wsIdListName.get(key)?.push(customer);
            }
        });

        return Array.from(wsIdListName.values());
    }
}