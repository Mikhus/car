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
} from '@imqueue/rpc';
import { CarObject } from './types';
import { CarsDB } from './CarsDB';
import { carPush, carSort, toPartial } from './helpers';

/**
 * Class Car - implements in-memory cars database with API access to cars data
 */
export class Car extends IMQService {

    /**
     * Cars in-memory database
     * @type {CarsDB}
     */
    private db: CarsDB;

    /**
     * Overrides and adds service-specific async stuff to service
     * initialization
     */
    @profile()
    public async start() {
        const ret = await super.start();
        const redis: IRedisClient = (this.imq as any).writer;

        this.db = new CarsDB(this.logger, redis);
        await this.db.bootstrap();

        return ret;
    }

    /**
     * Returns a list of car manufacturers (car brands)
     *
     * @return {string[]} - list of known brands
     */
    @profile()
    @expose()
    public brands(): string[] {
        return this.db.brands().sort();
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
        if (!(id instanceof Array)) {
            return toPartial(
                this.db.car(id),
                selectedFields || []
            );
        }

        return id.map(carId => toPartial(
            this.db.car(carId),
            selectedFields || [],
        ));
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
        return this.db.cars(brand)
            .sort(carSort(sort, dir))
            .reduce((cars, car) => carPush(cars, toPartial(car)), [])
        ;
    }

}
