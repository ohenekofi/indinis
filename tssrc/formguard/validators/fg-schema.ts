/**
 * @file tssrc/formguard/validators/schema.ts
 * @description A wrapper for adding pre/post processing to any FormGuard schema.
 */

import { FormGuardBase } from '../core';
import { FormGuardSchema, ValidationResult, TransformFunction } from '../types';

export class SchemaWrapper<T> extends FormGuardBase<T> {
    private preprocessFn: TransformFunction<any, any> | null = null;
    private postprocessFn: TransformFunction<T, any> | null = null;

    constructor(private coreSchema: FormGuardSchema<T>) {
        super();
    }

    protected _parse(value: any, path: (string | number)[]): ValidationResult<T> {
        // 1. Preprocess the data if a function is provided
        const preprocessedValue = this.preprocessFn ? this.preprocessFn(value) : value;

        // 2. Run the core schema's validation logic on the (potentially modified) data
        const result = this.coreSchema.parse(preprocessedValue);

        if (!result.success) {
            return result;
        }

        // 3. Postprocess the data if a function is provided and validation was successful
        const postprocessedData = this.postprocessFn ? this.postprocessFn(result.data) : result.data;
        
        return { success: true, data: postprocessedData };
    }
    
    /**
     * Adds a function to modify the input data *before* any validation occurs.
     * @param fn The preprocessing function.
     */
    public preprocess(fn: TransformFunction<any, any>): this {
        this.preprocessFn = fn;
        return this;
    }

    /**
     * Adds a function to modify the output data *after* all validation has succeeded.
     * @param fn The postprocessing function.
     */
    public postprocess<NewOutput>(fn: TransformFunction<T, NewOutput>): FormGuardSchema<NewOutput> {
        const newSchema = this.clone();
        newSchema.postprocessFn = fn;
        // The output type of the schema changes after postprocessing
        return newSchema as unknown as FormGuardSchema<NewOutput>;
    }

    protected clone(): this {
        const newWrapper = new SchemaWrapper(this.coreSchema) as this;
        newWrapper.steps = new Map(this.steps);
        newWrapper.preprocessFn = this.preprocessFn;
        newWrapper.postprocessFn = this.postprocessFn;
        return newWrapper;
    }
}