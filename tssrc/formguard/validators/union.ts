/**
 * @file tssrc/formguard/validators/union.ts
 * @description Union type validator for FormGuard.
 */

import { FormGuardBase } from '../core';
import { FormGuardSchema, ValidationResult, ValidationError, Infer } from '../types';

// <<< FIX #1: Define the generic type for the array of schemas.
// It can be any array of schemas.
type SchemaArray = readonly FormGuardSchema<any>[];

// <<< FIX #2: Infer the union of all possible output types from the array. >>>
type UnionOutput<T extends SchemaArray> = {
    [K in keyof T]: T[K] extends FormGuardSchema<any> ? Infer<T[K]> : never
}[number];

export class UnionValidator<T extends SchemaArray> extends FormGuardBase<UnionOutput<T>> {
    
    // <<< FIX #3: The constructor takes a non-empty array of schemas. >>>
    // We use a tuple type `[Schema, ...Schema[]]` to enforce non-emptiness at compile time.
    constructor(private schemas: readonly [T[0], ...T]) {
        super();
        if (!Array.isArray(schemas) || schemas.length === 0) {
            // This runtime check is a fallback for untyped JS usage.
            throw new Error("UnionValidator requires a non-empty array of schemas.");
        }
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<UnionOutput<T>> {
        const allErrors: ValidationError[] = [];
        
        for (const schema of this.schemas) {
            const result = schema.parse(value);
            if (result.success) {
                return result as ValidationResult<UnionOutput<T>>;
            } else {
                allErrors.push(...result.errors);
            }
        }
        
        return {
            success: false,
            errors: [{
                path,
                message: 'Input did not match any of the allowed types in the union',
                code: 'invalid_union',
                received: value
            }]
        };
    }

    protected clone(): this {
        const newValidator = new UnionValidator(this.schemas) as this;
        newValidator.steps = new Map(this.steps);
        return newValidator;
    }
}