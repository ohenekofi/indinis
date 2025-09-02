/**
 * @file tssrc/formguard/index.ts
 * @description Main entry point and factory for the internal FormGuard validation engine.
 * This module exports the `fg` object, which is used to construct all schemas.
 */

import { StringValidator } from './validators/string';
import { NumberValidator } from './validators/number';
import { BooleanValidator } from './validators/boolean';
import { DateValidator, DateValidatorConfig } from './validators/date';
import { ArrayValidator } from './validators/array';
import { ObjectValidator } from './validators/object';
import { EnumValidator } from './validators/enum';
import { UnionValidator } from './validators/union';
import { CustomValidator } from './validators/custom';
import { SchemaWrapper } from './validators/fg-schema';
import { FileValidator } from './validators/file';
import { FormDataParser } from './formData';
import { FormGuardSchema, CustomValidationContext, ValidationResult } from './types';

type SchemaArrayInput = readonly [FormGuardSchema<any>, ...FormGuardSchema<any>[]];

/**
 * The main FormGuard factory object. Use this to create all your schemas.
 *
 * @example
 * import { fg } from 'indinis';
 *
 * const schema = fg.object({
 *   name: fg.string().min(2),
 *   age: fg.number().int().positive()
 * });
 */
export const fg = {
    /**
     * Creates a new string validator.
     */
    string: () => new StringValidator(),
    
    /**
     * Creates a new number validator.
     */
    number: () => new NumberValidator(),

    /**
     * Creates a new boolean validator.
     */
    boolean: () => new BooleanValidator(),

    /**
     * Creates a new date validator.
     * @param config Optional configuration to handle specific formats, like Firestore Timestamps.
     */
    date: (config?: DateValidatorConfig) => new DateValidator(config),

    /**
     * Creates a new array validator.
     * @param elementSchema The schema to validate each element of the array against.
     */
    array: <T>(elementSchema: FormGuardSchema<T>) => new ArrayValidator<T>(elementSchema),

    /**
     * Creates a new object validator.
     * @param shape An object defining the schema for each key.
     */
    object: <T extends Record<string, FormGuardSchema<any>>>(shape: T) => {
        // This call is now type-correct.
        return new ObjectValidator(shape);
    },

    /**
     * Creates a validator for a fixed set of string literals (an enum).
     * @param values An array of allowed string values.
     */
    enum: <T extends string>(values: T[]) => new EnumValidator<T>(values),

    /**
     * Creates a validator that accepts any of the provided schemas.
     * @param schemas An array of `FormGuardSchema` instances.
     */
    union: <T extends SchemaArrayInput>(schemas: T) => {
        // This call is now type-correct and enforces the non-empty constraint.
        return new UnionValidator(schemas);
    },
    /**
     * Creates a schema with a custom validation function.
     * @param customValidator The function that performs the validation.
     */
    custom: <T>(customValidator: (value: any, ctx: CustomValidationContext) => ValidationResult<T>) => {
        return new CustomValidator<T>(customValidator);
    },
    
    /**
     * Wraps an existing schema to add preprocessing and postprocessing steps.
     * @param schema The core schema to wrap.
     */
    schema: <T>(schema: FormGuardSchema<T>) => new SchemaWrapper<T>(schema),
    
    /**
     * Creates a new file validator.
     * Note: This is primarily for use cases involving `FormData` and may have limited
     * applicability for direct JSON data in Indinis, but is included for completeness.
     */
    file: () => new FileValidator(),

    /**
     * Provides utilities for working directly with `FormData` objects.
     */
    formData: FormDataParser

    
};

// Re-export the Infer type for user convenience.
export { Infer } from './types';