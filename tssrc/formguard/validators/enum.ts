/**
 * @file tssrc/formguard/validators/enum.ts
 * @description Enum (string literal) validator for FormGuard.
 */

import { FormGuardBase } from '../core';
import { ValidationResult } from '../types';

export class EnumValidator<T extends string> extends FormGuardBase<T> {
    private allowedValues: Set<T>;

    constructor(private values: T[]) {
        super();
        if (!Array.isArray(values) || values.length === 0) {
            throw new Error("EnumValidator requires a non-empty array of string values.");
        }
        this.allowedValues = new Set(values);
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<T> {
        if (typeof value !== 'string') {
            return {
                success: false,
                errors: [{ path, message: 'Expected a string', code: 'invalid_type', received: value }]
            };
        }
        
        if (!this.allowedValues.has(value as T)) {
            return {
                success: false,
                errors: [{
                    path,
                    message: `Invalid value. Expected one of: ${this.values.join(', ')}`,
                    code: 'invalid_enum_value',
                    received: value
                }]
            };
        }
        
        return { success: true, data: value as T };
    }

    protected clone(): this {
        const newValidator = new EnumValidator(this.values) as this;
        newValidator.steps = new Map(this.steps);
        return newValidator;
    }
}