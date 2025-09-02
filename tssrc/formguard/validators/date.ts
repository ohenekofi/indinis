/**
 * @file tssrc/formguard/validators/date.ts
 * @description Date validator implementation for FormGuard, with support for cloud provider timestamp formats.
 */

import { FormGuardBase } from '../core';
import { ValidationResult } from '../types';

/**
 * A duck-typed interface for Firestore's Timestamp object.
 * We don't import the actual 'firebase-admin' type to keep FormGuard dependency-free.
 */
interface FirestoreTimestampLike {
    toDate(): Date;
    _seconds: number;
    _nanoseconds: number;
}

/**
 * Configuration for the DateValidator to handle different input types.
 */
export interface DateValidatorConfig {
    /**
     * Specifies the expected format of the input if it's not a standard type.
     * - 'firestore': The input can be a Firestore Timestamp object.
     */
    coerceFrom?: 'firestore';
}

export class DateValidator extends FormGuardBase<Date> {
    
    constructor(private config: DateValidatorConfig = {}) {
        super();
    }

    /**
     * Helper to check if an object looks like a Firestore Timestamp.
     */
    private isFirestoreTimestamp(value: any): value is FirestoreTimestampLike {
        return (
            typeof value === 'object' &&
            value !== null &&
            typeof value.toDate === 'function' &&
            typeof (value as any)._seconds === 'number' &&
            typeof (value as any)._nanoseconds === 'number'
        );
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<Date> {
        // 1. Handle standard JavaScript Date objects directly.
        if (value instanceof Date) {
            if (isNaN(value.getTime())) {
                return { success: false, errors: [{ path, message: 'Invalid Date object', code: 'invalid_date', received: value }] };
            }
            return { success: true, data: value };
        }

        // 2. Handle specific cloud provider formats if configured.
        if (this.config.coerceFrom === 'firestore' && this.isFirestoreTimestamp(value)) {
            // The toDate() method returns a JS Date, which we can then validate.
            const dateFromFirestore = value.toDate();
            if (isNaN(dateFromFirestore.getTime())) {
                return { success: false, errors: [{ path, message: 'Invalid Firestore Timestamp object', code: 'invalid_firestore_timestamp', received: value }] };
            }
            return { success: true, data: dateFromFirestore };
        }

        // 3. Handle common string (ISO 8601) and number (Unix timestamp in ms) formats.
        if (typeof value === 'string' || typeof value === 'number') {
            const date = new Date(value);
            if (isNaN(date.getTime())) {
                return { success: false, errors: [{ path, message: 'Invalid date string or timestamp', code: 'invalid_date_format', received: value }] };
            }
            return { success: true, data: date };
        }

        // 4. If none of the above, the type is invalid.
        return {
            success: false,
            errors: [{ 
                path, 
                message: this.config.coerceFrom === 'firestore'
                    ? 'Expected a Date, date string, timestamp, or Firestore Timestamp'
                    : 'Expected a Date, date string, or timestamp',
                code: 'invalid_type',
                received: value 
            }]
        };
    }
    
    /**
     * Checks if the date is on or after a minimum date.
     */
    public min(minDate: Date, message?: string): this {
        return this.refine(
            (date) => date.getTime() >= minDate.getTime(),
            message || `Date must be on or after ${minDate.toISOString()}`
        );
    }
    
    /**
     * Checks if the date is on or before a maximum date.
     */
    public max(maxDate: Date, message?: string): this {
        return this.refine(
            (date) => date.getTime() <= maxDate.getTime(),
            message || `Date must be on or before ${maxDate.toISOString()}`
        );
    }

    protected clone(): this {
        const newValidator = new DateValidator(this.config) as this;
        newValidator.steps = new Map(this.steps);
        return newValidator;
    }
}