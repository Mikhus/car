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
import { ILogger, IRedisClient, osUuid } from '@imqueue/rpc';
import { CarObject } from './types';
import {
    createReadStream,
    existsSync as exists,
    mkdirSync as mkdir,
} from 'fs';
import {
    CARS_DATA_UPDATE_INTERVAL,
    CARS_DATA_URL,
} from '../config';
import { execSync as exec } from 'child_process';
import { createInterface, ReadLine } from 'readline';

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
    'Small Sport Utility Vehicle 2WD': 'large',
};

/**
 * FieldDescription data type
 * @access private
 */
interface FieldDescription {
    name: string;
    pos: number;
}

/**
 * Fields map data type
 * @access private
 */
interface FieldsMap {
    [fieldName: string]: FieldDescription;
}

/**
 * In-memory cars database data structure
 * @access private
 */
interface CarsDBData {
    list: CarObject[];
    brands: { [name: string]: CarObject[] };
    hash: { [id: string]: CarObject };
    brandList: string[];
}

/**
 * In-Memory Cars Dictionary Database
 */
export class CarsDB {
    private data: CarsDBData = {
        list: [],
        brands: {},
        hash: {},
        brandList: [],
    };
    private dataDir: string = `${__dirname}/../data`;
    private dataZip: string = `${this.dataDir}/cars-db.zip`;
    private dataFile: string = `${this.dataDir}/vehicles.csv`;

    constructor(
        private readonly logger: ILogger,
        private readonly redis: IRedisClient,
    ) {}

    /**
     * Bootstraps the database routines
     */
    public async bootstrap() {
        if (!this.redis) {
            throw new Error('Redis connection lost');
        }
        const lock = await this.redis.set(
            `cars:db:lock:${osUuid()}`,
            '1', 'EX', 30, 'NX'
        );

        if (lock) {
            !exists(this.dataFile) && this.update();
            setInterval(() => this.update(), CARS_DATA_UPDATE_INTERVAL);
        }

        await this.load();
        await this.index();

        setInterval(async () => {
            await this.load();
            await this.index();
        }, CARS_DATA_UPDATE_INTERVAL);
    }

    /**
     * Updates database from remote source
     */
    public async update() {
        this.logger.log(`Updating cars database, pid ${process.pid}`);

        !exists(this.dataDir) && mkdir(this.dataDir);
        exec(`wget -O "${this.dataZip}" "${CARS_DATA_URL}"`);
        exec(`unzip -o ${this.dataZip} -d ${this.dataDir}`);
        exec(`rm ${this.dataZip}`);

        this.logger.log('Cars database updated!');
    }

    /**
     * Loads database data into memory
     */
    public async load() {
        return new Promise((resolve, reject) => {
            const map: FieldsMap = {
                make:  { name: 'make', pos: 0 },
                model: { name: 'model', pos: 0 },
                VClass: { name: 'type', pos: 0 },
                year: { name: 'year', pos: 0 },
            };
            const reader: ReadLine = createInterface({
                input: createReadStream(`${this.dataFile}`)
            });

            this.headerParsed = false;
            reader.on('line', this.parseLine.bind(this, map));
            reader.on('close', resolve);
            reader.on('error', reject);
        });
    }

    /**
     * Creates database indexes
     */
    public async index() {
        this.data.list.forEach((car: CarObject) => {
            if (!this.data.brands[car.make]) {
                this.data.brands[car.make] = [];
            }
            this.data.brands[car.make].push(car);
            this.data.hash[car.id] = car;
        });
        this.data.brandList = Object.keys(this.data.brands) || [];
    }

    /**
     * Returns car object by it's identifier
     * @param {string} id
     * @return {CarObject|null}
     */
    public car(id: string): CarObject | null {
        return this.data.hash[id] || null;
    }

    /**\
     * Returns list of car brands
     */
    public brands() {
        return this.data.brandList || [];
    }

    /**
     * Return list of cars for a given brand
     *
     * @param brand
     */
    public cars(brand: string) {
        return this.data.brands[brand] || [];
    }

    /**
     * Becomes true when the header found and parsed on db file load
     * @type {boolean}
     */
    private headerParsed = false;

    /**
     * Parses line of data from db file
     *
     * @param {FieldsMap} map
     * @param {string} line
     */
    private parseLine(map: FieldsMap, line: string) {
        const cols = line.split(',');

        if (!this.headerParsed) {
            this.parseHeader(cols, map);
            this.headerParsed = true;
            return ;
        }

        const car = this.createCarObject(map, cols);

        if (!car) {
            return ;
        }

        this.ensureListItem(car);
    }

    /**
     * Parses cars db file header line and fills up given map object
     *
     * @param {string[]} cols
     * @param {FieldsMap} map
     */
    private parseHeader(cols: string[], map: FieldsMap) {
        for (let i = 0; i < cols.length; i++) {
            if (map[cols[i]]) {
                map[cols[i]].pos = i;
            }
        }
    }

    /**
     * Builds car data object from a given cols using given fields map
     *
     * @param {FieldsMap} map
     * @param {string[]} cols
     * @return {CarObject}
     */
    private createCarObject(map: FieldsMap, cols: string[]) {
        const car = new CarObject();

        for (let col of Object.keys(map)) {
            if (map[col].name === 'year') {
                const year = cols[map[col].pos] as any | 0;

                if (isNaN(year)) {
                    return null;
                }

                car.years.push(year);
            }

            else if (map[col].name === 'make' && cols[map[col].pos] === '0') {
                return null;
            }

            let [name, value] = [map[col].name, cols[map[col].pos]];

            if (name === 'type') {
                value = TYPES_MAP[value];
            }

            (car as any)[name] = value;
        }

        return car;
    }

    /**
     * Ensures given item is not a given car duplicate. If so, checks if
     * car contains whole set of years from duplicate
     *
     * @param {CarObject} car
     * @param {CarObject} item
     * @return {boolean}
     */
    private ensureDuplicate(car: CarObject, item: CarObject) {
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
    }

    /**
     * Ensures given car object is a valid list item and if so -
     * pushes it to the list
     *
     * @param {CarObject} car
     */
    private ensureListItem(car: CarObject) {
        if (!this.data.list.find(this.ensureDuplicate.bind(this, car))) {
            car.id = hash.murmurHash128x64(String([
                car.make, car.model, car.type
            ]));
            this.data.list.push(car);
        }
    }

}
