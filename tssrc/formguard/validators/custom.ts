/**
 * @file tssrc/formguard/validators/custom.ts
 * @description A validator for user-defined custom logic, providing ultimate flexibility.
 */

import { FormGuardBase } from '../core';
import { ValidationResult, CustomValidationContext } from '../types';

/**
 * The signature for a user-provided custom validation function.
 * @template T The expected output type if validation is successful.
 * @param value The raw input value to be validated.
 * @param ctx The validation context, providing the path and error reporting utilities.
 * @returns A `ValidationResult` object.
 */
type CustomValidatorFn<T> = (value: any, ctx: CustomValidationContext) => ValidationResult<T>;

export class CustomValidator<T> extends FormGuardBase<T> {
    /**
     * @param validatorFn The user-provided function that contains the custom validation logic.
     */
    constructor(private validatorFn: CustomValidatorFn<T>) {
        super();
        if (typeof validatorFn !== 'function') {
            throw new Error("CustomValidator requires a function as its argument.");
        }
    }

    /**
     * The core parsing logic for a custom validator simply delegates to the
     * user-provided function.
     * @internal
     */
    protected _parse(value: any, path: (string | number)[]): ValidationResult<T> {
        // The context provides a way for the custom function to know its location
        // within a larger object and to add structured errors if needed.
        const context: CustomValidationContext = {
            path,
            // In a more complex engine, this could accumulate errors. For now,
            // we primarily rely on the function's return value.
            addIssue: (issue) => {
                // This is a placeholder for future enhancements where a custom validator
                // might report multiple, non-fatal issues.
                console.warn(`[FormGuard CustomValidator] addIssue called with:`, issue);
            },
        };

        try {
            // Execute the user's validation logic.
            return this.validatorFn(value, context);
        } catch (e: any) {
            // If the user's function throws an error, we catch it and convert it
            // into a standard FormGuard validation failure.
            return {
                success: false,
                errors: [{
                    path,
                    message: e.message || 'Custom validator threw an unexpected error',
                    code: 'custom_error',
                    received: value
                }]
            };
        }
    }
    
    /**
     * Clones the custom validator.
     * @internal
     */
    protected clone(): this {
        const newValidator = new CustomValidator(this.validatorFn) as this;
        // A custom validator can also have .refine() and .transform() steps, so we clone them.
        newValidator.steps = new Map(this.steps);
        return newValidator;
    }
}