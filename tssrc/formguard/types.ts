/**
 * @file tssrc/formguard/types.ts
 * @description Core type definitions for the internal FormGuard validation engine.
 */

// --- Core Validation Result Types ---

/**
 * Represents a single validation error.
 */
export interface ValidationError {
    /** The path to the field that failed validation (e.g., ['user', 'email']). */
    path: (string | number)[];
    /** A human-readable error message. */
    message: string;
    /** A machine-readable error code (e.g., 'invalid_type', 'too_small'). */
    code: string;
    /** The value that failed validation. */
    received?: any;
}

/**
 * The result of a successful validation.
 * @template T The expected output type.
 */
export interface ValidationSuccess<T> {
    success: true;
    /** The validated, sanitized, and correctly typed data. */
    data: T;
}

/**
 * The result of a failed validation.
 */
export interface ValidationFailure {
    success: false;
    /** An array of all validation errors that occurred. */
    errors: ValidationError[];
}

/**
 * The union type representing the outcome of any validation operation.
 * @template T The expected output type on success.
 */
export type ValidationResult<T> = ValidationSuccess<T> | ValidationFailure;

// --- Core Schema Interface ---

/**
 * The base interface for all schemas created by the `fg` engine.
 * This provides a common, identifiable type for a validator and is the
 * foundation of the entire FormGuard system.
 *
 * @template T The expected output type of the schema.
 */
export interface FormGuardSchema<T> {
    /**
     * Parses and validates the input data against the schema.
     * @param data The raw input data.
     * @returns A ValidationResult object containing either the typed data or an array of errors.
     */
    parse(data: any): ValidationResult<T>;

    /**
     * A unique marker property to identify this as a FormGuard schema,
     * aiding in type guards and runtime checks.
     * @internal
     */
    readonly _isFormGuardSchema: true;

    /**
     * The inferred output type of the schema. Used for type inference with `Infer<T>`.
     * @internal
     */
    _output: T;

    /**
     * Makes the schema optional. If the input is `null` or `undefined`,
     * validation will succeed and return `undefined`.
     */
    optional(): FormGuardSchema<T | undefined>;
    
    /**
     * Makes the schema nullable. If the input is `null`, validation will
     * succeed and return `null`.
     */
    nullable(): FormGuardSchema<T | null>;
}

// --- Utility and Context Types ---

/**
 * The context provided to custom validators.
 */
export interface CustomValidationContext {
    /** The current path within the data structure being validated. */
    path: (string | number)[];
    /** Adds an issue to the list of errors for the current validation run. */
    addIssue: (issue: Omit<ValidationError, 'path'>) => void;
}

/**
 * A function that transforms data. Can be used for preprocessing,
 * postprocessing, or in `.transform()` chains.
 */
export type TransformFunction<Input, Output> = (value: Input) => Output;

/**
 * A function that refines validation with a custom check.
 * It must return `true` for the validation to pass.
 */
export type RefineFunction<T> = (value: T) => boolean;

/**
 * Type helper to infer the TypeScript type from a FormGuardSchema.
 * 
 * @example
 * const mySchema = fg.object({ name: fg.string() });
 * type MyType = Infer<typeof mySchema>; // MyType is { name: string }
 */
export type Infer<T extends FormGuardSchema<any>> = T['_output'];


// --- Sanitization Types ---

/**
 * A custom function for sanitizing string inputs.
 */
export type CustomSanitizer = (value: string) => string;

/**
 * Configuration options for string sanitization.
 */
export interface StringSanitizeOptions {
    /** Removes leading and trailing whitespace. */
    trim?: boolean;
    /** Converts the entire string to lowercase. */
    lowercase?: boolean;
    /** Converts the entire string to uppercase. */
    uppercase?: boolean;
    /** Removes all whitespace characters from the string. */
    removeWhitespace?: boolean;
    /** A custom function to apply after all other sanitizations. */
    customSanitizer?: CustomSanitizer;
}