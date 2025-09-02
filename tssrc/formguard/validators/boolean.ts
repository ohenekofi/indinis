/**
 * @file tssrc/formguard/validators/boolean.ts
 * @description Boolean validator implementation for FormGuard.
 */

import { FormGuardBase } from '../core';
import { ValidationResult } from '../types';

// Sets of values that are coerced to a boolean.
// Using Sets provides fast lookups.
const TRUTHY_VALUES = new Set(['true', '1', 'yes', 'on']);
const FALSY_VALUES = new Set(['false', '0', 'no', 'off']);

export class BooleanValidator extends FormGuardBase<boolean> {

    /**
     * The core parsing logic for booleans. It is designed to be liberal
     * in what it accepts, coercing common string and number representations
     * to a boolean value.
     */
    protected _parse(value: any, path: (string | number)[]): ValidationResult<boolean> {
        // Direct boolean values are passed through.
        if (typeof value === 'boolean') {
            return { success: true, data: value };
        }

        // Handle common string representations from forms and APIs.
        if (typeof value === 'string') {
            const lowercased = value.toLowerCase().trim();
            if (TRUTHY_VALUES.has(lowercased)) {
                return { success: true, data: true };
            }
            if (FALSY_VALUES.has(lowercased)) {
                return { success: true, data: false };
            }
        }
        
        // Handle numeric representations.
        if (typeof value === 'number') {
            if (value === 1) return { success: true, data: true };
            if (value === 0) return { success: true, data: false };
        }

        // If the value is none of the above, it's considered an invalid type.
        return {
            success: false,
            errors: [{ 
                path, 
                message: 'Expected a boolean or a value coercible to a boolean (e.g., "true", 1, "on")', 
                code: 'invalid_type', 
                received: value 
            }]
        };
    }
    
    /**
     * Adds a refinement step to ensure the input value was strictly a boolean
     * (`true` or `false`) and not a coerced value like `"true"` or `1`.
     *
     * This is useful for APIs that require strict boolean types.
     *
     * @param message A custom error message.
     */
    public strict(message?: string): this {
        // Prepending this step with `addStep` is not quite right, as `_parse`
        // will have already coerced it. Instead, we refine based on the raw input.
        // A better approach is to wrap the original parse logic.
        
        const originalParse = this.parse.bind(this);
        
        this.parse = (data: any) => {
            if (typeof data !== 'boolean') {
                return {
                    success: false,
                    errors: [{
                        path: [],
                        message: message || 'Expected a strict boolean (true or false)',
                        code: 'invalid_type_strict',
                        received: data
                    }]
                };
            }
            return originalParse(data);
        };
        
        return this;
    }


    protected clone(): this {
        const newValidator = new BooleanValidator() as this;
        newValidator.steps = new Map(this.steps);
        // If we add more state, it must be cloned here.
        return newValidator;
    }
}