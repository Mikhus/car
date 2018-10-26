/**
 * Push given object to a given array and returns modified array
 *
 * @param {any[]} arr
 * @param {any} obj
 * @return {any[]}
 */
import { CarObject } from './types';

export function carPush(arr: any[], obj: any) {
    if (obj) {
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