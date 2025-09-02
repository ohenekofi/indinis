/**
 * @file tssrc/formguard/validators/array.ts
 * @description Array validator implementation for FormGuard.
 */

import { FormGuardBase } from '../core';
import { FormGuardSchema, ValidationResult, ValidationError } from '../types';

export class ArrayValidator<T> extends FormGuardBase<T[]> {
    
    constructor(private elementSchema: FormGuardSchema<T>) {
        super();
        if (!elementSchema || !elementSchema._isFormGuardSchema) {
            throw new Error("ArrayValidator requires a valid FormGuard schema as its element type.");
        }
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<T[]> {
        let inputArray: any[];

        // Handle the common case where a single value is submitted for a field
        // that is expected to be an array. Coerce it into a single-element array.
        if (Array.isArray(value)) {
            inputArray = value;
        } else if (value === undefined || value === null) {
            // An empty or missing value is treated as an empty array,
            // to be checked by refinements like .min() or .nonempty().
            inputArray = [];
        } else {
            // Coerce a single value into an array.
            inputArray = [value];
        }

        const validatedData: T[] = [];
        const allErrors: ValidationError[] = [];
        
        for (let i = 0; i < inputArray.length; i++) {
            const element = inputArray[i];
            const elementPath = [...path, i];

            // Validate each element against the provided sub-schema.
            const result = this.elementSchema.parse(element);

            if (result.success) {
                validatedData.push(result.data);
            } else {
                // Prepend the current array index to the path of any nested errors
                // to provide a full path to the invalid field (e.g., ['users', 0, 'email']).
                allErrors.push(...result.errors.map(e => ({ ...e, path: [i, ...e.path] })));
            }
        }
        
        if (allErrors.length > 0) {
            return { success: false, errors: allErrors };
        }

        return { success: true, data: validatedData };
    }

    /**
     * Checks if the array has a minimum number of elements.
     * @param length The minimum number of elements.
     * @param message A custom error message.
     */
    public min(length: number, message?: string): this {
        return this.refine(
            (arr) => arr.length >= length,
            message || `Array must contain at least ${length} element(s)`
        );
    }
    
    /**
     * A convenient shorthand for `.min(1)`.
     * @param message A custom error message.
     */
    public nonempty(message?: string): this {
        return this.min(1, message || 'Array must not be empty');
    }

    /**
     * Checks if the array has a maximum number of elements.
     * @param length The maximum number of elements.
     * @param message A custom error message.
     */
    public max(length: number, message?: string): this {
        return this.refine(
            (arr) => arr.length <= length,
            message || `Array must contain at most ${length} element(s)`
        );
    }
    
    /**
     * Checks if the array has an exact number of elements.
     * @param length The required number of elements.
     * @param message A custom error message.
     */
    public length(length: number, message?: string): this {
        return this.refine(
            (arr) => arr.length === length,
            message || `Array must contain exactly ${length} element(s)`
        );
    }

    protected clone(): this {
        // The elementSchema is an object, but it's treated as immutable.
        // A shallow copy is sufficient.
        const newValidator = new ArrayValidator(this.elementSchema) as this;
        newValidator.steps = new Map(this.steps);
        return newValidator;
    }
}