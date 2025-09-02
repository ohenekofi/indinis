/**
 * @file tssrc/formguard/validators/file.ts
 * @description File validator implementation for FormGuard.
 */

import { FormGuardBase } from '../core';
import { ValidationResult, ValidationError } from '../types';

// A minimal interface to represent a File object, making this code
// compatible in both Node.js (e.g., from Multer) and browser environments.
interface FileLike {
    name: string;
    size: number;
    type: string;
}

export class FileValidator extends FormGuardBase<FileLike | FileLike[]> {
    private allowedTypes: Set<string> | null = null;
    private minSizeBytes: number | null = null;
    private maxSizeBytes: number | null = null;
    private maxFileCount: number = 1; // Default to a single file

    protected _parse(value: any, path: (string | number)[]): ValidationResult<FileLike | FileLike[]> {
        const files: FileLike[] = Array.isArray(value) ? value : [value];

        if (files.some(f => !this.isFileLike(f))) {
            return {
                success: false,
                errors: [{ path, message: 'Expected a File or an array of Files', code: 'invalid_type', received: value }]
            };
        }

        if (files.length > this.maxFileCount) {
            return {
                success: false,
                errors: [{ path, message: `Expected at most ${this.maxFileCount} file(s), but received ${files.length}`, code: 'too_many_files' }]
            };
        }

        const allErrors: ValidationError[] = [];

        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            const filePath = [...path, i];

            if (this.allowedTypes && !this.allowedTypes.has(file.type)) {
                allErrors.push({
                    path: filePath,
                    message: `Invalid file type '${file.type}'. Allowed types are: ${[...this.allowedTypes].join(', ')}`,
                    code: 'invalid_file_type',
                    received: file.type
                });
            }

            if (this.minSizeBytes !== null && file.size < this.minSizeBytes) {
                allErrors.push({
                    path: filePath,
                    message: `File is too small. Minimum size is ${this.minSizeBytes} bytes, but file is ${file.size} bytes.`,
                    code: 'file_too_small',
                    received: file.size
                });
            }

            if (this.maxSizeBytes !== null && file.size > this.maxSizeBytes) {
                allErrors.push({
                    path: filePath,
                    message: `File is too large. Maximum size is ${this.maxSizeBytes} bytes, but file is ${file.size} bytes.`,
                    code: 'file_too_large',
                    received: file.size
                });
            }
        }

        if (allErrors.length > 0) {
            return { success: false, errors: allErrors };
        }

        // Return a single file if only one was expected and received, otherwise return the array.
        const resultData = this.maxFileCount === 1 && files.length <= 1 ? files[0] : files;
        
        // Handle the case where a single optional file is not provided
        if (resultData === undefined && this.maxFileCount === 1) {
             return { success: true, data: [] }; // No file, but valid if optional
        }


        return { success: true, data: resultData || [] };
    }

    /**
     * Checks if an object has the basic properties of a File.
     */
    private isFileLike(value: any): value is FileLike {
        return (
            typeof value === 'object' &&
            value !== null &&
            typeof value.name === 'string' &&
            typeof value.size === 'number' &&
            typeof value.type === 'string'
        );
    }
    
    /**
     * Specifies the allowed MIME types for the file(s).
     * @param types An array of MIME type strings (e.g., ['image/jpeg', 'application/pdf']).
     */
    public types(types: string[]): this {
        this.allowedTypes = new Set(types);
        return this;
    }

    /**
     * Sets the maximum allowed file size in bytes.
     */
    public maxSize(bytes: number): this {
        this.maxSizeBytes = bytes;
        return this;
    }

    /**
     * Sets the minimum allowed file size in bytes.
     */
    public minSize(bytes: number): this {
        this.minSizeBytes = bytes;
        return this;
    }

    /**
     * Allows multiple files to be uploaded.
     * @param maxCount The maximum number of files allowed. Defaults to Infinity if not specified.
     */
    public multiple(maxCount: number = Infinity): this {
        this.maxFileCount = maxCount;
        return this;
    }

    protected clone(): this {
        const newValidator = new FileValidator() as this;
        newValidator.steps = new Map(this.steps); 
        newValidator.allowedTypes = this.allowedTypes ? new Set(this.allowedTypes) : null;
        newValidator.minSizeBytes = this.minSizeBytes;
        newValidator.maxSizeBytes = this.maxSizeBytes;
        newValidator.maxFileCount = this.maxFileCount;
        return newValidator;
    }
}