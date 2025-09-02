/**
 * @file tssrc/formguard/validators/number.ts
 * @description Number validator implementation for FormGuard, including extended methods.
 */

import { FormGuardBase } from '../core';
import { ValidationResult } from '../types';

export class NumberValidator extends FormGuardBase<number> {
    
    protected _parse(value: any, path: (string | number)[]): ValidationResult<number> {
        let numValue: number;

        if (typeof value === 'number') {
            numValue = value;
        } else if (typeof value === 'string' && value.trim() !== '') {
            numValue = Number(value);
        } else {
            return {
                success: false,
                errors: [{ path, message: 'Expected a number or a numeric string', code: 'invalid_type', received: value }]
            };
        }

        if (isNaN(numValue) || !isFinite(numValue)) {
            return {
                success: false,
                errors: [{ path, message: 'Invalid number', code: 'invalid_number', received: value }]
            };
        }
        
        return { success: true, data: numValue };
    }

    /**
     * Checks if the number is an integer.
     */
    public int(message?: string): this {
        return this.refine(
            (val) => Number.isInteger(val),
            message || 'Expected an integer'
        );
    }

    /**
     * Checks if the number is greater than a minimum value (exclusive).
     */
    public gt(minValue: number, message?: string): this {
        return this.refine(
            (val) => val > minValue,
            message || `Number must be greater than ${minValue}`
        );
    }

    /**
     * Checks if the number is greater than or equal to a minimum value (inclusive).
     * Alias for `.min()`.
     */
    public gte(minValue: number, message?: string): this {
        return this.min(minValue, message);
    }

    /**
     * Checks if the number is greater than or equal to a minimum value (inclusive).
     */
    public min(minValue: number, message?: string): this {
        return this.refine(
            (val) => val >= minValue,
            message || `Number must be ${minValue} or greater`
        );
    }

    /**
     * Checks if the number is less than a maximum value (exclusive).
     */
    public lt(maxValue: number, message?: string): this {
        return this.refine(
            (val) => val < maxValue,
            message || `Number must be less than ${maxValue}`
        );
    }

    /**
     * Checks if the number is less than or equal to a maximum value (inclusive).
     * Alias for `.max()`.
     */
    public lte(maxValue: number, message?: string): this {
        return this.max(maxValue, message);
    }

    /**
     * Checks if the number is less than or equal to a maximum value (inclusive).
     */
    public max(maxValue: number, message?: string): this {
        return this.refine(
            (val) => val <= maxValue,
            message || `Number must be ${maxValue} or less`
        );
    }

    /**
     * Checks if the number is positive (> 0).
     */
    public positive(message?: string): this {
        return this.refine(
            (val) => val > 0,
            message || 'Number must be positive'
        );
    }

    /**
     * Checks if the number is non-negative (>= 0).
     */
    public nonNegative(message?: string): this {
        return this.refine(
            (val) => val >= 0,
            message || 'Number must be non-negative'
        );
    }

    /**
     * Checks if the number is negative (< 0).
     */
    public negative(message?: string): this {
        return this.refine(
            (val) => val < 0,
            message || 'Number must be negative'
        );
    }
    
    /**
     * Checks if the number is non-positive (<= 0).
     */
    public nonPositive(message?: string): this {
        return this.refine(
            (val) => val <= 0,
            message || 'Number must be non-positive'
        );
    }

    /**
     * Checks if the number is a multiple of a given value.
     */
    public multipleOf(multiple: number, message?: string): this {
        return this.refine(
            (val) => val % multiple === 0,
            message || `Number must be a multiple of ${multiple}`
        );
    }
    
    /**
     * Checks if the number is "safe" in the JavaScript sense (between `Number.MIN_SAFE_INTEGER` and `Number.MAX_SAFE_INTEGER`).
     */
    public safe(message?: string): this {
        return this.refine(
            (val) => Number.isSafeInteger(val),
            message || 'Number must be a safe integer'
        );
    }

    protected clone(): this {
        const newValidator = new NumberValidator() as this;
        newValidator.steps = new Map(this.steps);
        return newValidator;
    }
}