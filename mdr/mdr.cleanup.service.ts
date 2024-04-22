import { Inject, Injectable } from "@nestjs/common";
import * as fs from "fs";
import csv from "csv-parser";
import { Pool } from "pg";
import { UnProcessCustomerObject } from "./mdr.service";
import { ProcessCustomerObject } from "./mdr.service";
import { MdrService } from "./mdr.service";

type CleanCustomerObject = {
    id: number;
    shopId: string;
    firstName: string;
    lastName: string;
    email: string;
    address1: string;
    address2: string;
    city: string;
    state: string;
    zip: string;
    phone: string | null;
    customerType: string;
    okMarketing: boolean;
    birthday: Date | null;
    nameCode: string;
    addressCode: boolean;
    duplicateCode: boolean;
    authDate: Date;
}

@Injectable()
export class MdrCleanUpService {
  constructor(
      private readonly mdrService: MdrService,
      @Inject("DB_CONNECTION") private readonly db: Pool
  ) {}

  async cleanup(unProcessCustomers: UnProcessCustomerObject []): Promise <CleanCustomerObject []> {
      return unProcessCustomers.map(customer => {
          let nameCode =  {
              firstname: '',
              lastname: '',
              fullname: ''
          }

          const keywords = [
              "Associates",
              "Auto Body",
              "Autobody",
              "Center",
              "Company",
              "Corp",
              "Dept",
              "Enterprise",
              "Inc.",
              "Insurance",
              "Landscap",
              "LLC",
              "Motor",
              "Office",
              "Rental",
              "Repair",
              "Salvage",
              "Service",
              "Supply",
              "Tire",
              "Towing",
          ];

          if (/[-&,*^\/]|(\()|( and )|( OR )/i.test(customer.firstname)) {
              customer.firstname = customer.firstname
                .split(/[-&,*^\/]|(\()|( and )|( OR )/i)[0]
                .trim();
              nameCode.firstname = "New Name";
              if (
                /'\s|[@]/.test(customer.firstname) ||
                customer.firstname.trim().split(/\s/).length > 2
              ) {
                customer.firstname = "";
                nameCode.firstname = "Bad Name";
              } else if (
                customer.firstname.trim().length === 1 ||
                customer.firstname.trim().length === 0
              ) {
                customer.firstname = "";
                nameCode.firstname = "Bad Name";
              } else if (
                /\d/.test(customer.firstname) ||
                customer.firstname.includes("'S ") ||
                customer.firstname.includes("'s ")
              ) {
                customer.firstname = "";
                nameCode.firstname = "Bad Name";
              } else if (
                keywords.some((keyword) => customer.firstname.includes(keyword))
              ) {
                customer.firstname = "";
                nameCode.firstname = "Bad Name";
              } else if (
                /\bAuto\b/.test(customer.firstname) ||
                /\bCar\b/.test(customer.firstname) ||
                /\bInc\b/.test(customer.firstname) ||
                /\bTown\b/.test(customer.firstname)
              ) {
                customer.firstname = "";
                nameCode.firstname = "Bad Name";
              } else if (
                customer.firstname.trim().length > 12
              ) {
                customer.firstname = "";
                nameCode.firstname = "Bad Name";
              }
            } else if (
              /'\s|[@]/.test(customer.firstname) ||
              customer.firstname.trim().split(/\s/).length > 2
            ) {
              customer.firstname = "";
              nameCode.firstname = "Bad Name";
            } else if (
              customer.firstname.trim().length === 1 ||
              customer.firstname.trim().length === 0
            ) {
              customer.firstname = "";
              nameCode.firstname = "Bad Name";
            } else if (
              /\d/.test(customer.firstname) ||
              customer.firstname.includes("'S ") ||
              customer.firstname.includes("'s ")
            ) {
              customer.firstname = "";
              nameCode.firstname = "Bad Name";
            } else if (
              keywords.some((keyword) => customer.firstname.includes(keyword))
            ) {
              customer.firstname = "";
              nameCode.firstname = "Bad Name";
            } else if (
              /\bAuto\b/.test(customer.firstname) ||
              /\bCar\b/.test(customer.firstname) ||
              /\bInc\b/.test(customer.firstname) ||
              /\bTown\b/.test(customer.firstname)
            ) {
              customer.firstname = "";
              nameCode.firstname = "Bad Name";
            } else if (
              customer.firstname.trim().length > 12
            ) {
              customer.firstname = "";
              nameCode.firstname = "Bad Name";
            } else {
              nameCode.firstname = "";
            }
      
            if (/[-,*^\/]/.test(customer.lastname)) {
              let splitName = customer.lastname.split(/[-,*^\/]/);
              if (splitName[1].length === 0) {
                customer.lastname = splitName[0].trim();
                if (customer.lastname.includes(" OR ")) {
                  customer.lastname = customer.lastname.split(" OR ")[1];
                }
              } else {
                customer.lastname = splitName[1].trim();
                if (customer.lastname.includes(" OR ")) {
                  customer.lastname = customer.lastname.split(" OR ")[1];
                }
              }
              nameCode.lastname = "New Name";
              if (
                /[@]|[&]|(\))/.test(customer.lastname) ||
                customer.lastname.trim().length === 1
              ) {
                customer.lastname = "";
                nameCode.lastname = "Bad Name";
              } else if (
                /\d/.test(customer.lastname) ||
                customer.lastname.includes("'S ") ||
                customer.lastname.includes("'s ") ||
                customer.lastname.split(".").length > 2
              ) {
                customer.lastname = "";
                nameCode.lastname = "Bad Name";
              } else if (
                customer.lastname.trim().length > 14
              ) {
                customer.lastname = "";
                nameCode.lastname = "Bad Name";
              }
            } else if (
              /[@]|[&]|(\))/.test(customer.lastname) ||
              customer.lastname.trim().length === 1 ||
              customer.lastname.trim().length === 0
            ) {
              customer.lastname = "";
              nameCode.lastname = "Bad Name";
            } else if (
              /\d/.test(customer.lastname) ||
              customer.lastname.includes("'S ") ||
              customer.lastname.includes("'s ") ||
              customer.lastname.split(".").length > 2
            ) {
              customer.lastname = "";
              nameCode.lastname = "Bad Name";
            } else if (
              customer.lastname.trim().length > 14
            ) {
              customer.lastname = "";
              nameCode.lastname = "Bad Name";
            } else {
              nameCode.lastname = "";
            }
      
            if (
              nameCode.firstname == "Bad Name" ||
              nameCode.lastname == "Bad Name"
            ) {
              nameCode.fullname = "Bad Name";
            } else if (
              nameCode.firstname == "New Name" ||
              nameCode.lastname == "New Name"
            ) {
              nameCode.fullname = "New Name";
            } else {
              nameCode.fullname = "";
            }
      
            return {
              id: customer.id,
              shopId: customer.shopid,
              firstName: customer.firstname,
              lastName: customer.lastname,
              email: customer.email,
              address1: customer.address1,
              address2: customer.address2,
              city: customer.city,
              state: customer.state,
              zip: customer.zip,
              phone: customer.phone,
              customerType: customer.customertype,
              okMarketing: customer.okformarketing,
              birthday: customer.birthday,
              nameCode: nameCode.fullname,
              addressCode: (customer.address1.trim() === '' || customer.address1 === null ) ? false : true,
              duplicateCode: false,
              authDate: new Date(customer.authdate)
            };
      })
  }

  async deduplicateUnProcessedCustomers(customers: CleanCustomerObject []): Promise<CleanCustomerObject []> {
      const uniqueCustomersMap = new Map<string, CleanCustomerObject>();
      let sortedCustomers: CleanCustomerObject [] = [];
      try {

        sortedCustomers = customers.sort((a, b) =>  new Date(b.authDate).getTime() - new Date(a.authDate).getTime());
      } catch (err) {
        console.log(sortedCustomers.filter(customer => customer.authDate === null))
      }

      sortedCustomers.forEach(customer => {
          const {
              firstName,
              lastName,
              address1,
              shopId,
              authDate
          } = customer;

          const key = `${firstName.toLocaleLowerCase().trim()}-${lastName.toLocaleLowerCase().trim()}-${address1.toLocaleLowerCase().trim()}-${shopId}`;

          if (!uniqueCustomersMap.has(key) || (new Date(uniqueCustomersMap.get(key)?.authDate ?? ``) < new Date(authDate))) {
              uniqueCustomersMap.set(key, customer);
          }
      });

      return Array.from(uniqueCustomersMap.values());
  }

  async deduplicateProcessedCustomers(customers: ProcessCustomerObject []): Promise<ProcessCustomerObject []> {
    const uniqueCustomersMap = new Map<string, ProcessCustomerObject>();
    let sortedCustomers: ProcessCustomerObject [] = [];
      try {

        sortedCustomers = customers.sort((a, b) =>  new Date(b.authdate).getTime() - new Date(a.authdate).getTime());
      } catch (err) {
        console.log(sortedCustomers.filter(customer => customer.authdate === null))
      }
    // const sortedCustomers = customers.sort((a, b) => new Date(b.authdate).getTime() - new Date(a.authdate).getTime());
    sortedCustomers.forEach(customer => {
      const {
        firstname,
        lastname,
        address,
        wsid,
        authdate
      } = customer;

      const key = `${firstname.toLocaleLowerCase().trim()}-${lastname.toLocaleLowerCase().trim()}-${address.toLocaleLowerCase().trim()}-${wsid}`;

      if (!uniqueCustomersMap.has(key) || (new Date(uniqueCustomersMap.get(key)?.authdate ?? ``) < new Date(authdate))) {
          uniqueCustomersMap.set(key, customer);
      }
    });

    return Array.from(uniqueCustomersMap.values());
  }

  async getCleanCustomers(tekShopId: number, wsId: string): Promise <{
    process: ProcessCustomerObject [];
    unprocess: CleanCustomerObject [];
  }> {
    const rawCustomers = await this.mdrService.spliteTekCustomers(tekShopId, wsId);

    const cleanUnProcessCustomers = await this.cleanup(rawCustomers.unprocess);
    const deduplicateUnProcessCustomers = await this.deduplicateUnProcessedCustomers(cleanUnProcessCustomers);
    const duplicatedProcessCustomers = await this.deduplicateProcessedCustomers(rawCustomers.process);

    return {
      process: duplicatedProcessCustomers,
      unprocess:deduplicateUnProcessCustomers
    }
  }
}