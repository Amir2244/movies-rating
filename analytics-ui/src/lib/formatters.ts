/**
 * @file A collection of pure utility functions for formatting data for display.
 * Keeping these separate makes them easy to test and reuse throughout the app.
 */

/**
 * Formats a number of bytes into a human-readable string (e.g., KB, MB, GB).
 * @param bytes - The number of bytes.
 * @returns A formatted string.
 */
export const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/**
 * Formats a number by adding commas as thousand separators.
 * @param num - The number to format.
 * @returns A formatted string.
 */
export const formatNumber = (num: number): string => {
    if (typeof num !== 'number' || isNaN(num)) {
        return '0';
    }
    return num.toLocaleString();
};

/**
 * Extracts a numeric value from potentially complex MongoDB-style objects.
 * Handles cases like {$numberLong: "123"} or {$date: "..."}.
 * @param value - The value to parse, which can be a number or an object.
 * @returns The extracted number.
 */
/* eslint-disable */
export const extractValue = (value: any): number => {
    if (typeof value === 'object' && value !== null) {
        if (value.$numberLong) return parseInt(value.$numberLong, 10);
        if (value.$date) return new Date(value.$date).getTime();
    }
    return typeof value === 'number' ? value : 0;
};

