/**
 * @file tssrc/formguard/validators/string.ts
 * @description String validator implementation for FormGuard.
 */

import { FormGuardBase } from '../core';
import { StringSanitizeOptions, ValidationResult } from '../types';

export class StringValidator extends FormGuardBase<string> {
    private sanitizeOptions: StringSanitizeOptions = {};

    constructor() {
        super();
        // This initial step ensures that all subsequent refinements operate on a string.
        this.addStep((value: any, path) => {
            if (typeof value !== 'string') {
                return {
                    success: false,
                    errors: [{ path, message: 'Expected a string', code: 'invalid_type', received: value }]
                };
            }
            return { success: true, data: value };
        }, "type_check");
    }
    
    protected _parse(value: any, path: (string | number)[]): ValidationResult<string> {
        let processedValue = value;

        if (typeof value !== 'string') {
            if (value === null || value === undefined) {
                // Let .optional() or .nullable() handle this.
            } else if (typeof value === 'number' || typeof value === 'boolean') {
                processedValue = String(value); // Coerce from primitives
            } else {
                 return {
                    success: false,
                    errors: [{ path, message: 'Expected a string or a value convertible to a string', code: 'invalid_type', received: value }]
                };
            }
        }
        
        let sanitized = processedValue as string;
        if (this.sanitizeOptions.trim) sanitized = sanitized.trim();
        if (this.sanitizeOptions.lowercase) sanitized = sanitized.toLowerCase();
        if (this.sanitizeOptions.uppercase) sanitized = sanitized.toUpperCase();
        if (this.sanitizeOptions.removeWhitespace) sanitized = sanitized.replace(/\s/g, '');
        if (this.sanitizeOptions.customSanitizer) sanitized = this.sanitizeOptions.customSanitizer(sanitized);

        return { success: true, data: sanitized };
    }
    
    /**
     * Sets sanitization rules for the string. These are applied before any validation.
     */
    public sanitize(options: StringSanitizeOptions): this {
        this.sanitizeOptions = { ...this.sanitizeOptions, ...options };
        return this;
    }
    
    /**
     * Checks if the string has a minimum length.
     */
    public min(length: number, message?: string): this {
        return this.refine(
            (val) => val.length >= length,
            message || `String must be at least ${length} characters long`
        );
    }

    /**
     * Checks if the string has a maximum length.
     */
    public max(length: number, message?: string): this {
        return this.refine(
            (val) => val.length <= length,
            message || `String must be at most ${length} characters long`
        );
    }
    
    /**
     * Checks if the string has an exact length.
     */
    public length(length: number, message?: string): this {
        return this.refine(
            (val) => val.length === length,
            message || `String must be exactly ${length} characters long`
        );
    }

    /**
     * Checks if the string is a valid email address.
     */
    public email(message?: string): this {
        // A commonly used, reasonably effective email regex.
        const emailRegex = /^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
        return this.refine(
            (val) => emailRegex.test(val),
            message || 'Invalid email format'
        );
    }

    /**
     * Checks if the string is a valid URL.
     */
    public url(message?: string): this {
        return this.refine(
            (val) => {
                try {
                    new URL(val);
                    return true;
                } catch (_) {
                    return false;
                }
            },
            message || 'Invalid URL format'
        );
    }

    /**
     * Checks if the string is a valid UUID.
     */
    public uuid(message?: string): this {
        const uuidRegex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
        return this.refine(
            (val) => uuidRegex.test(val),
            message || 'Invalid UUID format'
        );
    }
    
    /**
     * Checks if the string is a valid ISO 8601 date-time string.
     */
    public datetime(message?: string): this {
        return this.refine(
            (val) => !isNaN(Date.parse(val)),
            message || 'Invalid date-time format'
        );
    }
    
    /**
     * Checks if the string matches a given regular expression.
     */
    public regex(regex: RegExp, message?: string): this {
        return this.refine(
            (val) => regex.test(val),
            message || 'String does not match the required pattern'
        );
    }

    /**
     * Checks if the string contains a given substring.
     */
    public includes(substring: string, message?: string): this {
        return this.refine(
            (val) => val.includes(substring),
            message || `String must include "${substring}"`
        );
    }
    
    /**
     * Checks if the string starts with a given substring.
     */
    public startsWith(substring: string, message?: string): this {
        return this.refine(
            (val) => val.startsWith(substring),
            message || `String must start with "${substring}"`
        );
    }
    
    /**
     * Checks if the string ends with a given substring.
     */
    public endsWith(substring: string, message?: string): this {
        return this.refine(
            (val) => val.endsWith(substring),
            message || `String must end with "${substring}"`
        );
    }

    protected clone(): this {
        const newValidator = new StringValidator() as this;
        newValidator.steps = new Map(this.steps);
        newValidator.sanitizeOptions = { ...this.sanitizeOptions };
        return newValidator;
    }
}