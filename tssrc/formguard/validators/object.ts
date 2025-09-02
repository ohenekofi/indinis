/**
 * @file tssrc/formguard/validators/object.ts
 * @description Object validator implementation for FormGuard, with advanced methods.
 */

import { FormGuardBase } from '../core';
import { FormGuardSchema, ValidationResult, ValidationError, Infer } from '../types';

type ObjectShape = Record<string, FormGuardSchema<any>>;

// Helper type to extract keys from a generic type T
type Keys<T> = T extends any ? keyof T : never;

export class ObjectValidator<T extends ObjectShape> extends FormGuardBase<{ [K in keyof T]: Infer<T[K]> }> {
    
    // --- Configuration flags for advanced behavior ---
    private shouldStrip = false;
    private shouldRejectUnknown = false;

    constructor(private shape: T) {
        super();
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<any> {
        if (typeof value !== 'object' || value === null || Array.isArray(value)) {
            return {
                success: false,
                errors: [{ path, message: 'Expected an object', code: 'invalid_type', received: value }]
            };
        }

        const validatedData: Record<string, any> = {};
        const allErrors: ValidationError[] = [];
        
        // Handle unknown keys based on configuration
        const inputKeys = new Set(Object.keys(value));
        const schemaKeys = new Set(Object.keys(this.shape));

        if (this.shouldRejectUnknown) {
            for (const key of inputKeys) {
                if (!schemaKeys.has(key)) {
                    allErrors.push({
                        path: [...path, key],
                        message: `Unrecognized key '${key}'`,
                        code: 'unrecognized_key'
                    });
                }
            }
        }

        // Validate each key defined in the schema shape
        for (const key in this.shape) {
            const fieldSchema = this.shape[key];
            const fieldValue = value[key];
            const fieldPath = [...path, key];

            const result = fieldSchema.parse(fieldValue);

            if (result.success) {
                if (result.data !== undefined) {
                    validatedData[key] = result.data;
                }
            } else {
                allErrors.push(...result.errors.map(e => ({ ...e, path: [key, ...e.path] })));
            }
        }

        // If not stripping, copy over unknown keys (unless in strict mode)
        if (!this.shouldStrip && !this.shouldRejectUnknown) {
            for (const key of inputKeys) {
                if (!schemaKeys.has(key)) {
                    validatedData[key] = value[key];
                }
            }
        }
        
        if (allErrors.length > 0) {
            return { success: false, errors: allErrors };
        }

        return { success: true, data: validatedData };
    }

    /**
     * Disallows any keys that are not explicitly defined in the schema.
     * If unknown keys are present, validation will fail.
     */
    public strict(): this {
        this.shouldRejectUnknown = true;
        this.shouldStrip = false; // Strict and strip are mutually exclusive
        return this;
    }

    /**
     * Silently removes any keys that are not explicitly defined in the schema.
     */
    public strip(): this {
        this.shouldStrip = true;
        this.shouldRejectUnknown = false; // Strict and strip are mutually exclusive
        return this;
    }

    /**
     * Creates a new schema by adding additional fields to the current one.
     * @param extensionShape An object containing the new schemas to add.
     */
    public extend<E extends ObjectShape>(extensionShape: E): ObjectValidator<T & E> {
        const newShape = { ...this.shape, ...extensionShape };
        return new ObjectValidator(newShape);
    }

    /**
     * Creates a new schema containing only a subset of the original fields.
     * @param keysToPick An array of keys to keep.
     */
    public pick<K extends Keys<T>>(keysToPick: K[]): ObjectValidator<Pick<T, K>> {
        const newShape: Partial<Pick<T, K>> = {};
        for (const key of keysToPick) {
            if (this.shape[key]) {
                newShape[key] = this.shape[key];
            }
        }
        return new ObjectValidator(newShape as Pick<T, K>);
    }

    /**
     * Creates a new schema containing all but a subset of the original fields.
     * @param keysToOmit An array of keys to remove.
     */
    public omit<K extends Keys<T>>(keysToOmit: K[]): ObjectValidator<Omit<T, K>> {
        const newShape = { ...this.shape };
        for (const key of keysToOmit) {
            delete newShape[key];
        }
        return new ObjectValidator(newShape as Omit<T, K>);
    }

    protected clone(): this {
        const newValidator = new ObjectValidator({ ...this.shape }) as this;
        newValidator.steps = new Map(this.steps);
        newValidator.shouldStrip = this.shouldStrip;
        newValidator.shouldRejectUnknown = this.shouldRejectUnknown;
        return newValidator;
    }
}