/**
 * @file tssrc/formguard/formData.ts
 * @description Provides utilities for parsing and validating web FormData objects,
 *              handling common conventions for arrays and nested objects.
 */

import { FormGuardSchema, ValidationResult } from './types';

/**
 * Parses a FormData object into a plain JavaScript object suitable for validation.
 * It intelligently handles common form encoding patterns:
 * - `name="tags[]"` becomes a `tags` array.
 * - `name="user[name]"` becomes a nested `user` object.
 *
 * @param formData The FormData object, typically from a web request.
 * @returns A structured JavaScript object.
 */
function parseFormData(formData: FormData): Record<string, any> {
    const obj: Record<string, any> = {};

    for (const [key, value] of formData.entries()) {
        // --- 1. Handle Array Syntax: key[] ---
        if (key.endsWith('[]')) {
            const arrayKey = key.slice(0, -2);
            // Initialize the array if it doesn't exist
            if (!obj[arrayKey]) {
                obj[arrayKey] = [];
            }
            // Ensure it's an array before pushing
            if (Array.isArray(obj[arrayKey])) {
                obj[arrayKey].push(value);
            }
            continue; // Move to the next entry
        }

        // --- 2. Handle Nested Object Syntax: parent[child] ---
        // This regex captures the parent key and the child key.
        const nestedMatch = key.match(/^([^[]+)\[([^\]]+)\]$/);
        if (nestedMatch) {
            const parentKey = nestedMatch[1];
            const childKey = nestedMatch[2];
            
            // Initialize the parent object if it doesn't exist
            if (!obj[parentKey] || typeof obj[parentKey] !== 'object' || Array.isArray(obj[parentKey])) {
                obj[parentKey] = {};
            }
            
            // Assign the value to the nested property.
            // Note: This simple parser doesn't handle deeper nesting like parent[child][grandchild]
            // or arrays of objects like items[0][name]. It's designed for common, one-level deep forms.
            obj[parentKey][childKey] = value;
            continue; // Move to the next entry
        }
        
        // --- 3. Handle Simple Key ---
        // If a key appears multiple times without array syntax, FormData standard
        // dictates `getAll` should be used. `entries()` iterates over all values.
        // If the key already exists, we convert it to an array.
        if (obj.hasOwnProperty(key)) {
            if (!Array.isArray(obj[key])) {
                // Convert the existing single value to an array
                obj[key] = [obj[key]];
            }
            obj[key].push(value);
        } else {
            obj[key] = value;
        }
    }

    return obj;
}


/**
 * A static object providing utility methods for working with FormData.
 * This is exported as part of the main `fg` object.
 *
 * @example
 * import { fg } from 'indinis';
 *
 * const result = fg.formData.validate(request.formData(), mySchema);
 */
export const FormDataParser = {
    /**
     * Parses a FormData object into a structured JavaScript object.
     */
    parse: parseFormData,

    /**
     * Parses a FormData object and then validates it against a FormGuard schema.
     * This is the primary entry point for validating form submissions.
     * @param formData The FormData object to validate.
     * @param schema The FormGuard schema to use for validation.
     * @returns A ValidationResult object.
     */
    validate: <T>(formData: FormData, schema: FormGuardSchema<T>): ValidationResult<T> => {
        try {
            const parsedData = parseFormData(formData);
            return schema.parse(parsedData);
        } catch (e: any) {
            // This catches potential errors from the parsing logic itself, though it's designed to be safe.
            return {
                success: false,
                errors: [{
                    path: [],
                    message: `Failed to parse FormData: ${e.message}`,
                    code: 'formdata_parsing_error'
                }]
            };
        }
    }
};