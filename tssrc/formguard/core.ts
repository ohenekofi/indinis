/**
 * @file tssrc/formguard/core.ts
 * @description Contains the FormGuardBase class, the abstract foundation with
 *              common logic and chainable methods for all validator schemas.
 */

import {
    FormGuardSchema,
    ValidationResult,
    TransformFunction,
    RefineFunction
} from './types';

type ValidationStep<T> = (value: T, path: (string | number)[]) => ValidationResult<T>;

/**
 * @internal
 * An abstract base class for all FormGuard schemas.
 */
export abstract class FormGuardBase<T> implements FormGuardSchema<T> {
    public readonly _isFormGuardSchema = true;
    public _output!: T;
    protected steps: Map<string, ValidationStep<any>> = new Map();

    protected abstract _parse(value: any, path: (string | number)[]): ValidationResult<any>;
    protected abstract clone(): this;

    protected addStep(step: ValidationStep<any>, key: string): void {
        this.steps.set(key, step);
    }

    public parse(data: any): ValidationResult<T> {
        const rootPath: (string | number)[] = [];
        const initialResult = this._parse(data, rootPath);
        if (!initialResult.success) {
            return initialResult;
        }
        let currentData: any = initialResult.data;

        for (const step of this.steps.values()) {
            const stepResult = step(currentData, rootPath);
            if (!stepResult.success) {
                return stepResult;
            }
            currentData = stepResult.data;
        }
        return { success: true, data: currentData as T };
    }
    
    public refine(check: RefineFunction<T>, message: string): this {
        const refineKey = `refine_${this.steps.size}`;
        this.addStep((value: T, path) => {
            if (!check(value)) {
                return {
                    success: false,
                    errors: [{ path, message, code: 'custom_refinement_failed', received: value }]
                };
            }
            return { success: true, data: value };
        }, refineKey);
        return this;
    }

    public transform<NewOutput>(transformFn: TransformFunction<T, NewOutput>): FormGuardSchema<NewOutput> {
        const newSchema = this.clone();
        const transformKey = `transform_${newSchema.steps.size}`;
        newSchema.addStep((value: T, path) => {
            try {
                return { success: true, data: transformFn(value) };
            } catch (e: any) {
                return {
                    success: false,
                    errors: [{ path, message: e.message || 'Transformation failed', code: 'transform_error', received: value }]
                };
            }
        }, transformKey);
        return newSchema as unknown as FormGuardSchema<NewOutput>;
    }
    


    /**
     * Makes the schema optional. If the input is `null` or `undefined`,
     * validation will succeed and return `undefined`.
     */
    public optional(): FormGuardSchema<T | undefined> {
        // We create a new schema that wraps the current one.
        // This new schema's output type is correctly `T | undefined`.
        return new OptionalSchema(this);
    }

    /**
     * Makes the schema nullable. If the input is `null`, validation will succeed
     * and return `null`.
     */
    public nullable(): FormGuardSchema<T | null> {
        // Similar to optional, we wrap the current schema.
        return new NullableSchema(this);
    }
}

/**
 * @internal
 * A private wrapper schema that implements the logic for `.optional()`.
 */
class OptionalSchema<T> extends FormGuardBase<T | undefined> {
    constructor(private wrappedSchema: FormGuardSchema<T>) {
        super();
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<T | undefined> {
        if (value === undefined || value === null) {
            return { success: true, data: undefined };
        }
        // If the value is not null/undefined, delegate to the original wrapped schema.
        return this.wrappedSchema.parse(value);
    }

    protected clone(): this {
        // Cloning an optional schema just creates a new optional schema with the same inner schema.
        return new OptionalSchema(this.wrappedSchema) as this;
    }
}

/**
 * @internal
 * A private wrapper schema that implements the logic for `.nullable()`.
 */
class NullableSchema<T> extends FormGuardBase<T | null> {
    constructor(private wrappedSchema: FormGuardSchema<T>) {
        super();
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<T | null> {
        if (value === null) {
            return { success: true, data: null };
        }
        // If the value is not null, delegate to the original wrapped schema.
        // This will correctly fail for `undefined` unless `.optional()` was also chained.
        return this.wrappedSchema.parse(value);
    }

    protected clone(): this {
        return new NullableSchema(this.wrappedSchema) as this;
    }
}
