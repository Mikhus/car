/*!
 * ISC License
 * 
 * Copyright (c) 2018, Imqueue Sandbox
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
import {
    IMQService,
    IRedisClient,
    expose,
    profile,
    osUuid,
} from '@imqueue/rpc';
import { execSync as exec } from 'child_process';
import { createInterface, ReadLine } from 'readline';
import { createReadStream, existsSync as exists, mkdirSync as mkdir } from 'fs';
import { CARS_DATA_URL, CARS_DATA_UPDATE_INTERVAL } from '../config';
import { CarObject } from './types';

const hash: any = require('murmurhash-native');
const TYPES_MAP: { [name: string]: string } = {
    'Two Seaters': 'mini',
    'Subcompact Cars': 'mini',
    Vans: 'large',
    'Compact Cars': 'midsize',
    'Midsize Cars': 'midsize',
    'Large Cars': 'large',
    'Small Station Wagons': 'large',
    'Midsize-Large Station Wagons': 'large',
    'Small Pickup Trucks': 'large',
    'Standard Pickup Trucks': 'large',
    'Special Purpose Vehicle 2WD': 'large',
    'Special Purpose Vehicles': 'large',
    'Minicompact Cars': 'mini',
    'Special Purpose Vehicle 4WD': 'large',
    'Midsize Station Wagons': 'large',
    'Small Pickup Trucks 2WD': 'large',
    'Standard Pickup Trucks 2WD': 'large',
    'Standard Pickup Trucks 4WD': 'large',
    'Minivan - 2WD': 'large',
    'Sport Utility Vehicle - 4WD': 'large',
    'Minivan - 4WD': 'large',
    'Sport Utility Vehicle - 2WD': 'large',
    'Small Pickup Trucks 4WD': 'large',
    'Standard Pickup Trucks/2wd': 'large',
    'Vans Passenger': 'large',
    'Special Purpose Vehicles/2wd': 'large',
    'Special Purpose Vehicles/4wd': 'large',
    'Small Sport Utility Vehicle 4WD': 'large',
    'Standard Sport Utility Vehicle 2WD': 'large',
    'Standard Sport Utility Vehicle 4WD': 'large',
    'Small Sport Utility Vehicle 2WD': 'large'
};

/**
 * Class Car - implements in-memory cars database with API access to cars data
 */
export class Car extends IMQService {

    private dataDir: string = `${__dirname}/../data`;
    private dataZip: string = `${this.dataDir}/cars-db.zip`;
    private dataFile: string = `${this.dataDir}/vehicles.csv`;

    private data: {
        list: CarObject[],
        brands: { [name: string]: CarObject[] },
        hash: { [id: string]: CarObject },
    } = { list: [], brands: {}, hash: {} };

    /**
     * Overrides and adds service-specific async stuff to service
     * initialization
     */
    @profile()
    public async start() {
        const ret = await super.start();
        const redis: IRedisClient = (this.imq as any).writer;

        if (redis) {
            const lock = await redis.set(
                `cars:db:lock:${osUuid()}`,
                '1', 'EX', 30, 'NX'
            );

            if (lock) {
                !exists(this.dataFile) && this.updateDb();
                setInterval(() => this.updateDb(), CARS_DATA_UPDATE_INTERVAL);
            }
        }

        await this.loadDb();
        await this.indexDb();

        setInterval(async () => {
            await this.loadDb();
            await this.indexDb();
        }, CARS_DATA_UPDATE_INTERVAL);

        return ret;
    }

    /**
     * Loads database data from remote service and save is to data directory
     *
     * @access private
     * @return {Promise<void>}
     */
    @profile()
    private updateDb() {
        this.logger.log(`Updating cars database, pid ${process.pid}`);

        !exists(this.dataDir) && mkdir(this.dataDir);
        exec(`wget -O "${this.dataZip}" "${CARS_DATA_URL}"`);
        exec(`unzip -o ${this.dataZip} -d ${this.dataDir}`);
        exec(`rm ${this.dataZip}`);

        this.logger.log('Cars database updated!');
    }

    /**
     * Loads database into memory
     *
     * @access private
     * @return {Promise<void>}
     */
    @profile()
    private async loadDb() {
        return new Promise((resolve, reject) => {
            const reader: ReadLine = createInterface({
                input: createReadStream(`${this.dataFile}`)
            });
            const map: any = {
                make:  { name: 'make', pos: 0 },
                model: { name: 'model', pos: 0 },
                VClass: { name: 'type', pos: 0 },
                year: { name: 'year', pos: 0 },
            };
            let count = 0;

            reader.on('line', (line: string) => {
                const cols = line.split(',');

                if (count === 0) {
                    for (let i = 0; i < cols.length; i++) {
                        if (map[cols[i]]) {
                            map[cols[i]].pos = i;
                        }
                    }

                    return ++count;
                }

                const car = new CarObject();

                for (let col of Object.keys(map)) {
                    if (map[col].name === 'year') {
                        const year = parseInt(cols[map[col].pos], 10);
                        if (isNaN(year)) {
                            return ;
                        }
                        car.years.push(year);
                    } else {
                        if (
                            map[col].name === 'make' &&
                            cols[map[col].pos] === '0'
                        ) {
                            return ;
                        }

                        let [name, value] = [map[col].name, cols[map[col].pos]];

                        if (name === 'type') {
                            value = TYPES_MAP[value];
                        }

                        (car as any)[name] = value;
                    }
                }

                if (!this.data.list.find((item) => {
                    const dup = (
                        item.make === car.make &&
                        item.model === car.model &&
                        item.type === car.type
                    );

                    if (dup) {
                        item.years = (item.years.concat(car.years));
                        item.years = item.years.filter(
                            (elem, pos) => item.years.indexOf(elem) === pos
                        );

                        item.years.sort();
                    }

                    return dup;
                })) {
                    car.id = hash.murmurHash128x64(String([
                        car.make, car.model, car.type
                    ]));
                    this.data.list.push(car);
                }
            });
            reader.on('close', resolve);
            reader.on('error', reject);
        });
    }

    /**
     * Indexes in-memory data for optimized access
     *
     * @access private
     * @return {Promise<void>}
     */
    @profile()
    private async indexDb() {
        this.data.list.forEach((car: CarObject) => {
            if (!this.data.brands[car.make]) {
                this.data.brands[car.make] = [];
            }
            this.data.brands[car.make].push(car);
            this.data.hash[car.id] = car;
        });
    }

    /**
     * Returns a list of car manufacturers (car brands)
     *
     * @return {string[]} - list of known brands
     */
    @profile()
    @expose()
    public brands(): string[] {
        return Object.keys(this.data.brands).sort();
    }

    /**
     * Constructs partial car object from a given car object
     *
     * @param {CarObject | null} car
     * @param {string[]} selectedFields
     * @return {Partial<CarObject> | null}
     * @access private
     */
    @profile()
    private partialCar(
        car: CarObject | null,
        selectedFields: string[]
    ): Partial<CarObject> | null {
        if (!car) {
            return null;
        }

        let newCar: Partial<CarObject> | null = car;

        if (car && selectedFields && selectedFields.length) {
            newCar = {};

            selectedFields.forEach((field: string) => {
                if ((car as any)[field] !== undefined) {
                    (newCar as any)[field] = (car as any)[field];
                }
            });
        }

        return newCar;
    }

    /**
     * Returns car object by its identifier or if multiple identifiers given
     * as array of identifiers - returns a list of car objects.
     *
     * @param {string | string[]} id - car identifier
     * @param {string[]} [selectedFields] - fields to return
     * @return {Partial<CarObject> | Partial<CarObject|null>[] | null} - found object or null otherwise
     */
    @profile()
    @expose()
    public fetch(
        id: string | string[],
        selectedFields?: string[]
    ): Partial<CarObject> | Partial<CarObject|null>[] | null {
        if (!(id instanceof Array && id.length)) {
            return this.partialCar(
                this.data.hash[id as string] || null,
                selectedFields || []
            );
        }

        const cars: Partial<CarObject | null>[] = [];

        for (let carId of id) {
            cars.push(this.partialCar(
                this.data.hash[carId] || null,
                selectedFields || []
            ));
        }

        return cars;
    }

    /**
     * Returns list of known cars for a given brand
     *
     * @param {string} brand - car manufacturer (brand) name
     * @param {string[]} [selectedFields] - fields to return
     * @param {string} [sort] - sort field, by default is 'model'
     * @param {'asc' | 'desc'} [dir] - sort direction, by default is 'asc' - ascending
     * @return {Partial<CarObject>[]} - list of found car objects
     */
    @profile()
    @expose()
    public list(
        brand: string,
        selectedFields?: string[],
        sort: string = 'model',
        dir: 'asc' | 'desc' = 'asc',
    ): Partial<CarObject>[] {
        let cars = (this.data.brands[brand] || []).sort((a: any, b: any) => {
            if (a[sort] < b[sort]) {
                return dir === 'asc' ? -1 : 1;
            } else if (a[sort] > b[sort]) {
                return dir === 'asc' ? 1 : -1;
            } else {
                return 0;
            }
        });

        if (selectedFields && selectedFields.length) {
            cars = cars.map((car: CarObject) => {
                const newCar: any = {};
                selectedFields.forEach((field: string) => {
                    newCar[field] = (car as any)[field];
                });

                return newCar as CarObject;
            });
        }

        return cars;
    }
}
