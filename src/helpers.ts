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
import { CarObject } from './types';

/**
 * Push given object to a given array and returns modified array
 *
 * @param {any[]} arr
 * @param {any} obj
 * @return {any[]}
 */
export function carPush(arr: any[], obj: any) {
    if (obj && !arr.find(item => obj.id === item.id)) {
        arr.push(obj);
    }

    return arr;
}

/**
 * Returns car sort handler
 *
 * @param {string[]} sortField
 * @param {'asc'|'desc'} dir
 */
export function carSort(sortField: string, dir: 'asc' | 'desc') {
    return (a: any, b: any) => {
        if (a[sortField] < b[sortField]) {
            return dir === 'asc' ? -1 : 1;
        }

        else if (a[sortField] > b[sortField]) {
            return dir === 'asc' ? 1 : -1;
        }

        return 0;
    };
}

/**
 * Constructs partial car object from a given car object
 *
 * @param {CarObject | null} car
 * @param {string[]} selectedFields
 * @return {Partial<CarObject> | null}
 * @access private
 */
export function toPartial(
    car: CarObject | null,
    selectedFields?: string[]
): Partial<CarObject> | null {
    if (!car) {
        return null;
    }

    if (!(car && selectedFields && selectedFields.length)) {
        return car;
    }

    const newCar: Partial<CarObject> | null = {};

    for (let field of selectedFields) {
        if ((car as any)[field] !== undefined) {
            (newCar as any)[field] = (car as any)[field];
        }
    }

    return newCar;
}