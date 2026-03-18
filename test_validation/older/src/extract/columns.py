import sqlparse
from sqlparse.sql import Identifier, Function, TokenList, Comment # Ensure necessary imports
from sqlparse.tokens import Keyword, Name, Punctuation # Ensure necessary imports

# --- Updated Helper Function to Resolve Identifiers ---
# --- Updated Helper Function to Resolve Identifiers ---
def _resolve_sql_identifier_helper(token, alias_map, debug=False):
    """
    Resolves a sqlparse Identifier token to (table, column).
    Handles aliases. Returns (None, None) if not a clear column identifier.
    Explicitly ignores keywords and tries to handle quoted names.
    """
    if debug:
        print(f"[DEBUG] _resolve_sql_identifier_helper: Processing token type={type(token)}, value='{token.value}', ttype={token.ttype}")
    
    # --- Rule 1: Ignore keywords explicitly ---
    if token.is_keyword:
        if debug:
            print(f"[DEBUG] Skipping keyword: '{token.value}'")
        return None, None
    
    # --- Rule 1.5: Enhanced function detection ---
    # Check for common SQL functions that should not be treated as columns
    common_functions = {
        'CAST', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'SUBSTR', 'REPLACE',
        'YEAR', 'MONTH', 'DAY', 'DATE', 'TIME', 'NOW', 'CURRENT_DATE',
        'UPPER', 'LOWER', 'LENGTH', 'TRIM', 'ROUND', 'FLOOR', 'CEIL',
        'COALESCE', 'ISNULL', 'CONCAT', 'SUBSTRING', 'CHARINDEX'
    }
    
    # Check if token value starts with a function name followed by parenthesis
    if token.ttype is Name:
        val_upper = token.value.upper()
        if val_upper in common_functions:
            if debug:
                print(f"[DEBUG] Skipping function name: '{token.value}'")
            return None, None
    
    # Check for function patterns in the value itself
    if hasattr(token, 'value'):
        import re
        # Pattern: FUNCTION_NAME( ... anything ... )
        func_pattern = r'^(' + '|'.join(common_functions) + r')\s*\('
        if re.match(func_pattern, token.value, re.IGNORECASE):
            if debug:
                print(f"[DEBUG] Skipping function call pattern: '{token.value}'")
            return None, None

    if isinstance(token, Identifier):
        parent_name = token.get_parent_name() # Potential alias or table
        real_name = token.get_real_name()     # Column name (should handle quotes)
        
        if debug:
            print(f"[DEBUG] Identifier: parent_name='{parent_name}', real_name='{real_name}'")

        # --- Enhanced function filtering for real_name ---
        if real_name and real_name.upper() in common_functions:
            if debug:
                print(f"[DEBUG] Ignoring function name in real_name: '{real_name}'")
            return None, None

        # --- Check if the identifier looks like a function call ---
        if real_name and '(' in token.value and ')' in token.value:
            # If it contains parentheses, it's likely a function call, not a column reference
            if debug:
                print(f"[DEBUG] Skipping potential function call: '{token.value}'")
            return None, None

        # --- NEW: Handle ORDER BY clause contamination ---
        # If parent_name is None but token value contains table.column pattern with ASC/DESC
        if parent_name is None and '.' in token.value:
            # Try to manually parse the identifier when sqlparse fails
            token_value = token.value.strip()
            # Remove ORDER BY keywords (ASC, DESC, NULLS FIRST, NULLS LAST, etc.)
            order_keywords = ['ASC', 'DESC', 'NULLS', 'FIRST', 'LAST']
            clean_value = token_value
            for keyword in order_keywords:
                # Remove keyword if it appears as a separate word (with word boundaries)
                import re
                pattern = r'\b' + re.escape(keyword) + r'\b'
                clean_value = re.sub(pattern, '', clean_value, flags=re.IGNORECASE).strip()
            
            if debug:
                print(f"[DEBUG] ORDER BY cleaning: '{token_value}' -> '{clean_value}'")
            
            # Now try to parse the cleaned value
            if '.' in clean_value:
                parts = clean_value.split('.')
                if len(parts) == 2:
                    manual_parent = parts[0].strip()
                    manual_real = parts[1].strip()
                    if manual_parent and manual_real:
                        # Additional check: make sure this isn't a function call
                        if not ('(' in clean_value and ')' in clean_value):
                            if debug:
                                print(f"[DEBUG] Manual parsing successful: parent='{manual_parent}', real='{manual_real}'")
                            parent_name = manual_parent
                            real_name = manual_real

        # --- Rule 2: Ignore if real_name itself is a common keyword/function found by mistake ---
        if real_name and real_name.upper() in ('ASC', 'DESC', 'NULL', 'REAL', 'AS', 'ON', 'CAST', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN'): # Add more SQL keywords/common functions
             if debug:
                 print(f"[DEBUG] Ignoring '{real_name}' based on keyword/function list.")
             return None, None # Don't treat these as columns

        # --- Rule 3: If parent exists, resolve it; otherwise, table is None ---
        resolved_table = alias_map.get(parent_name, parent_name) if parent_name else None
        
        if debug:
            print(f"[DEBUG] Resolved table: '{resolved_table}' (from parent_name='{parent_name}')")

        # --- Rule 4: Return resolved table and the real column name ---
        if real_name:
             if debug:
                 print(f"[DEBUG] Returning: ({resolved_table}, {real_name})")
             return resolved_table, real_name
        else:
             if debug:
                 print(f"[DEBUG] No valid real_name, returning (None, None)")
             return None, None # No valid column name extracted

    # --- Rule 5: Handle specific non-Identifier types that might represent columns ---
    # Check ttype for Name tokens that might be quoted identifiers
    elif token.ttype is Name: # CORRECTION HERE: Check ttype
         if debug:
             print(f"[DEBUG] Name token: '{token.value}'")
         # Check if it looks like a quoted identifier (heuristic)
         val = token.value
         if (val.startswith('`') and val.endswith('`')) or \
            (val.startswith('"') and val.endswith('"')) or \
            (val.startswith('[') and val.endswith(']')):
               # Cannot determine table context here, return (None, quoted_name)
               if debug:
                   print(f"[DEBUG] Quoted identifier found: (None, {val})")
               return None, val
         else:
               # Unquoted Name is likely a keyword or function name already filtered, ignore.
               if debug:
                   print(f"[DEBUG] Unquoted Name ignored: '{val}'")
               return None, None

    # Other types are not simple columns
    if debug:
        print(f"[DEBUG] Token type {type(token)} not handled, returning (None, None)")
    return None, None

# --- Updated Helper Function to Recursively Extract Columns ---
def extract_columns_recursive(token, alias_map, columns_set, known_tables_and_aliases, debug=False, depth=0):
    """
    Recursively traverses sqlparse tokens to find column references.
    Avoids processing table/alias identifiers themselves as columns.

    Args:
        token: The current sqlparse token/TokenList to process.
        alias_map: Dictionary mapping aliases to real table names.
        columns_set: The set to add found (table, column) tuples to.
        known_tables_and_aliases: A set containing real table names and alias names found in FROM/JOIN.
        debug: Enable debug output.
        depth: Current recursion depth for indented debug output.
    """
    indent = "  " * depth
    if debug:
        print(f"{indent}[DEBUG] extract_columns_recursive: depth={depth}, token='{token.value}', type={type(token)}")
    
    # --- Base case 1: If it's an identifier ---
    if isinstance(token, Identifier):
        # --- Check: Is this identifier likely the *declaration* of a table/alias? ---
        parent_identifier = token.get_parent_name()
        real_name = token.get_real_name()
        if debug:
            print(f"{indent}[DEBUG] Identifier found: real_name='{real_name}', parent='{parent_identifier}'")
        
        # Heuristic: If it has no parent AND its name is a known table/alias,
        # it's likely the table name itself in FROM/JOIN. Don't treat as column.
        if parent_identifier is None and real_name in known_tables_and_aliases:
            # Don't add (table, table) or (alias, alias)
            if debug:
                print(f"{indent}[DEBUG] Skipping Identifier (likely table/alias name): '{real_name}'")
            pass # Skip processing this as a column
        else:
            # Otherwise, try to resolve it as a potential column reference (e.g., alias.column or just column)
            table, column = _resolve_sql_identifier_helper(token, alias_map, debug)
            if debug:
                print(f"{indent}[DEBUG] Resolved Identifier to: {(table, column)}")
            if column: # Add only if a valid column name was returned
                columns_set.add((table, column))
                if debug:
                    print(f"{indent}[DEBUG] Added to columns_set: {(table, column)}")
        # Stop recursion for identifiers; process them directly here.

    # --- Base case 2: Handle potential quoted Name tokens ---
    # (Included check from resolver, but maybe needed here if resolver misses it?)
    elif token.ttype is Name:
        if debug:
            print(f"{indent}[DEBUG] Name token found: '{token.value}'")
        table, column = _resolve_sql_identifier_helper(token, alias_map, debug)
        if column: # Add only if a valid (quoted) column name was returned
              columns_set.add((table, column))
              if debug:
                  print(f"{indent}[DEBUG] Added to columns_set: {(table, column)}")

    # --- Recursive step: If it's a list-like object, process its children ---
    # Exclude Identifier itself to avoid infinite loops (handled above)
    elif hasattr(token, 'tokens') and not isinstance(token, Identifier):
        if debug:
            print(f"{indent}[DEBUG] Recursing into TokenList: {type(token)}")
        for sub_token in token.tokens:
             if not sub_token.is_whitespace and not isinstance(sub_token, Comment):
                  # Pass down the known tables/aliases for context
                  extract_columns_recursive(sub_token, alias_map, columns_set, known_tables_and_aliases, debug, depth + 1)
    else:
        if debug:
            print(f"{indent}[DEBUG] Ignoring token (not Identifier, Name, or recursible List): '{token.value}'")

# --- Main Function to Orchestrate Column Extraction (No major changes needed here) ---
def extract_all_columns(parsed, alias_map, known_tables_and_aliases, debug=False):
    """
    Extracts all referenced columns from various parts of the parsed SQL.

    Args:
        parsed: The parsed sqlparse Statement object.
        alias_map: Dictionary mapping aliases to real table names.
        known_tables_and_aliases: Set of table names and alias names found in FROM/JOIN.
        debug: Enable debug output.

    Returns:
        A set of tuples {(resolved_table_name, column_name)}.
        Table name can be None if unresolved.
    """
    if debug:
        print(f"[DEBUG] extract_all_columns: Starting extraction")
        print(f"[DEBUG] alias_map: {alias_map}")
        print(f"[DEBUG] known_tables_and_aliases: {known_tables_and_aliases}")
    
    columns = set()
    extract_columns_recursive(parsed, alias_map, columns, known_tables_and_aliases, debug)

    if debug:
        print(f"[DEBUG] extract_all_columns: Final columns set: {columns}")

    return columns # Return the direct result first for debugging


def extract_columns(parsed, alias_map, resolved_tables, known_tables_and_aliases, debug=False):
    """
    Wrapper function to extract columns and perform final resolution.

    Args:
        parsed: The parsed sqlparse Statement object.
        alias_map: Dictionary mapping aliases to real table names.
        resolved_tables: Set of resolved table names (or subquery placeholders) from FROM/JOIN.
        known_tables_and_aliases: Set combining resolved_tables and alias names.
        debug: Enable debug output.

    Returns:
        Set of final (resolved_table, column_name) tuples.
        Returns None or raises exception on error (depending on desired handling).
    """
    try:
        if debug:
            print(f"[DEBUG] extract_columns: Starting with resolved_tables={resolved_tables}")
        
        # Pass the alias map AND the set of known table/alias names
        all_found_columns = extract_all_columns(parsed, alias_map, known_tables_and_aliases, debug)

        # Final resolution attempt for unqualified columns if context allows
        final_columns = set()
        # Filter out subquery placeholders from potential context tables
        non_subquery_tables = {t for t in resolved_tables if not (isinstance(t, str) and t.endswith("(subquery)"))}

        if debug:
            print(f"[DEBUG] non_subquery_tables: {non_subquery_tables}")

        for table, col in all_found_columns:
            resolved_table = table
            # If table context is missing AND only one actual table exists
            if table is None and len(non_subquery_tables) == 1:
                 resolved_table = next(iter(non_subquery_tables))
                 if debug:
                     print(f"[DEBUG] Resolved unqualified column '{col}' to table '{resolved_table}'")
            # Only add if column seems valid (not None)
            if col:
                final_columns.add((resolved_table, col))

        if debug:
            print(f"[DEBUG] extract_columns: Final result: {final_columns}")

        return final_columns
    except Exception as e:
        if debug:
            print(f"[DEBUG] extract_columns: Exception occurred: {e}")
        raise e # Raise the exception for handling upstream

# Test function to demonstrate the issue
def test_sql_parsing():
    """
    Test function to demonstrate the A11 parsing issue
    """
    
    sql = """SELECT schools.City FROM schools INNER JOIN frpm ON schools.CDSCode = frpm.CDSCode WHERE schools.EILCode = 'HS'   AND schools.County = 'Merced'   AND frpm.CDSCode = 'Lunch Provision 2'   AND schools.LowestGrade = 9   AND schools.HighestGrade = 12;"""
    
    print(f"[DEBUG] Testing SQL: {sql}")
    parsed = sqlparse.parse(sql)[0]
    
    # Mock data for testing
    alias_map = {}  # No aliases in this query
    resolved_tables = {'schools', 'frpm'}
    known_tables_and_aliases = {'schools', 'frpm'}
    
    # Extract columns with debug enabled
    result = extract_columns(parsed, alias_map, resolved_tables, known_tables_and_aliases, debug=True)
    
    print(f"\n[RESULT] Extracted columns: {result}")
    
    # Check specifically for A11
    a11_entries = [col for col in result if col[1] == 'A11']
    print(f"[RESULT] A11 entries: {a11_entries}")

def comprehensive_test_suite():
    """
    Comprehensive test suite to validate the ORDER BY fix robustness
    """
    print("\n" + "="*60)
    print("COMPREHENSIVE TEST SUITE: ORDER BY Parsing Robustness")
    print("="*60)
    
    test_cases = [
        # Basic cases that should work
        {
            "name": "Simple ORDER BY with ASC",
            "sql": "SELECT name FROM users ORDER BY users.id ASC",
            "expected_columns": {('users', 'name'), ('users', 'id')},
            "check_column": 'id',
            "expected_table": 'users'
        },
        {
            "name": "Simple ORDER BY with DESC", 
            "sql": "SELECT name FROM users ORDER BY users.created_date DESC",
            "expected_columns": {('users', 'name'), ('users', 'created_date')},
            "check_column": 'created_date',
            "expected_table": 'users'
        },
        {
            "name": "Multiple ORDER BY columns",
            "sql": "SELECT name FROM users ORDER BY users.name ASC, users.id DESC",
            "expected_columns": {('users', 'name'), ('users', 'id')},
            "check_column": 'id',
            "expected_table": 'users'
        },
        {
            "name": "ORDER BY without ASC/DESC",
            "sql": "SELECT name FROM users ORDER BY users.name",
            "expected_columns": {('users', 'name')},
            "check_column": 'name',
            "expected_table": 'users'
        },
        {
            "name": "ORDER BY with NULLS FIRST",
            "sql": "SELECT name FROM users ORDER BY users.score DESC NULLS FIRST",
            "expected_columns": {('users', 'name'), ('users', 'score')},
            "check_column": 'score',
            "expected_table": 'users'
        },
        {
            "name": "ORDER BY with NULLS LAST", 
            "sql": "SELECT name FROM users ORDER BY users.rating ASC NULLS LAST",
            "expected_columns": {('users', 'name'), ('users', 'rating')},
            "check_column": 'rating',
            "expected_table": 'users'
        },
        # Edge cases that might break
        {
            "name": "Column name contains DESC",
            "sql": "SELECT name FROM users ORDER BY users.description ASC",
            "expected_columns": {('users', 'name'), ('users', 'description')},
            "check_column": 'description',
            "expected_table": 'users'
        },
        {
            "name": "Column name contains ASC",
            "sql": "SELECT name FROM users ORDER BY users.ascii_code DESC", 
            "expected_columns": {('users', 'name'), ('users', 'ascii_code')},
            "check_column": 'ascii_code',
            "expected_table": 'users'
        },
        # Original problematic case
        {
            "name": "Original district.A11 case",
            "sql": "SELECT client.gender FROM client ORDER BY district.A11 DESC",
            "expected_columns": {('client', 'gender'), ('district', 'A11')},
            "check_column": 'A11', 
            "expected_table": 'district'
        },
        # Cases that should NOT be affected by our fix
        {
            "name": "Normal qualified column (no ORDER BY)",
            "sql": "SELECT users.name FROM users WHERE users.id = 1",
            "expected_columns": {('users', 'name'), ('users', 'id')},
            "check_column": 'name',
            "expected_table": 'users'
        }
    ]
    
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n[TEST {i}] {test_case['name']}")
        print(f"SQL: {test_case['sql']}")
        
        try:
            parsed = sqlparse.parse(test_case['sql'])[0]
            
            # Extract all unique table names for test setup
            import re
            tables_in_sql = set(re.findall(r'\b(\w+)\.', test_case['sql']))
            tables_in_sql.update(re.findall(r'FROM\s+(\w+)', test_case['sql'], re.IGNORECASE))
            tables_in_sql.update(re.findall(r'JOIN\s+(\w+)', test_case['sql'], re.IGNORECASE))
            
            alias_map = {}
            resolved_tables = tables_in_sql
            known_tables_and_aliases = tables_in_sql
            
            # Extract columns (with debug disabled for cleaner output)
            extracted = extract_columns(parsed, alias_map, resolved_tables, known_tables_and_aliases, debug=False)
            
            print(f"Extracted: {extracted}")
            
            # Check if the specific column has correct table attribution
            target_entries = [col for col in extracted if col[1] == test_case['check_column']]
            
            success = False
            if target_entries:
                actual_table = target_entries[0][0]
                expected_table = test_case['expected_table']
                success = actual_table == expected_table
                print(f"Target column '{test_case['check_column']}' -> Table: {actual_table} (Expected: {expected_table})")
            else:
                print(f"ERROR: Target column '{test_case['check_column']}' not found!")
            
            results.append({
                'test': test_case['name'],
                'success': success,
                'extracted': extracted,
                'target_column': test_case['check_column'],
                'expected_table': test_case['expected_table'],
                'actual_table': target_entries[0][0] if target_entries else None
            })
            
            print(f"Result: {'✓ PASS' if success else '✗ FAIL'}")
            
        except Exception as e:
            print(f"ERROR: {e}")
            results.append({
                'test': test_case['name'],
                'success': False,
                'error': str(e)
            })
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for r in results if r['success'])
    total = len(results)
    
    print(f"Passed: {passed}/{total}")
    
    for result in results:
        status = "✓ PASS" if result['success'] else "✗ FAIL"
        print(f"{status} {result['test']}")
        if not result['success'] and 'actual_table' in result:
            print(f"    Expected: {result['expected_table']}, Got: {result['actual_table']}")
    
    print(f"\nOverall Success Rate: {passed/total*100:.1f}%")
    
    # Analysis
    if passed == total:
        print("\n🎉 All tests passed! The fix appears to be robust and systematic.")
    else:
        print(f"\n⚠️  {total-passed} test(s) failed. The fix may need refinement.")
    
    return results

def test_function_parsing():
    """
    Test function to specifically address function parsing issues
    """
    print("\n" + "="*60)
    print("FUNCTION PARSING TEST")
    print("="*60)
    
    # Your problematic SQL
    sql = "SELECT AVG(COUNT(frpm.CDSCode)) AS average_test_takers FROM schools INNER JOIN frpm ON schools.CDSCode = frpm.CDSCode WHERE YEAR(schools.OpenDate) = 1980;"
    
    print(f"[DEBUG] Testing SQL: {sql}")
    parsed = sqlparse.parse(sql)[0]
    
    # Mock data for testing
    alias_map = {}
    resolved_tables = {'schools', 'frpm'}
    known_tables_and_aliases = {'schools', 'frpm'}
    
    # Extract columns with debug enabled
    result = extract_columns(parsed, alias_map, resolved_tables, known_tables_and_aliases, debug=True)
    
    print(f"\n[RESULT] Extracted columns: {result}")
    
    # Analyze the results
    print("\n[ANALYSIS]")
    expected_columns = {
        ('frpm', 'CDSCode'),     # From JOIN and nested function
        ('schools', 'CDSCode'),  # From JOIN condition
        ('schools', 'OpenDate')  # From WHERE clause
    }
    
    print(f"Expected columns: {expected_columns}")
    
    # Check for unwanted function names in results
    function_names_found = []
    for table, col in result:
        if col.upper() in ['YEAR', 'AVG', 'COUNT']:
            function_names_found.append((table, col))
    
    if function_names_found:
        print(f"❌ Function names incorrectly identified as columns: {function_names_found}")
    else:
        print("✅ No function names misidentified as columns")
    
    # Check for malformed identifiers
    malformed = []
    for table, col in result:
        if '(' in col or ')' in col or 'AS' in col:
            malformed.append((table, col))
    
    if malformed:
        print(f"❌ Malformed column identifiers found: {malformed}")
    else:
        print("✅ No malformed column identifiers")
    
    # Check if we got the core columns we need
    core_columns_found = 0
    for expected_table, expected_col in expected_columns:
        if (expected_table, expected_col) in result:
            core_columns_found += 1
            print(f"✅ Found expected column: {(expected_table, expected_col)}")
        else:
            print(f"❌ Missing expected column: {(expected_table, expected_col)}")
    
    success_rate = core_columns_found / len(expected_columns) * 100
    print(f"\nCore columns found: {core_columns_found}/{len(expected_columns)} ({success_rate:.1f}%)")
    
    return result

if __name__ == "__main__":
    test_sql_parsing()
    
    # Additional focused test for ORDER BY issue
    print("\n" + "="*50)
    print("FOCUSED TEST: ORDER BY clause parsing")
    print("="*50)
    
    sql_orderby = "SELECT client.gender FROM client ORDER BY client.birth_date ASC, district.A11 DESC"
    print(f"[DEBUG] Testing ORDER BY SQL: {sql_orderby}")
    parsed_orderby = sqlparse.parse(sql_orderby)[0]
    
    alias_map = {}
    resolved_tables = {'client', 'district'}
    known_tables_and_aliases = {'client', 'district'}
    
    result_orderby = extract_columns(parsed_orderby, alias_map, resolved_tables, known_tables_and_aliases, debug=True)
    print(f"\n[RESULT] ORDER BY test - Extracted columns: {result_orderby}")
    
    # Check for A11 and birth_date
    a11_entries = [col for col in result_orderby if col[1] == 'A11']
    birth_date_entries = [col for col in result_orderby if col[1] == 'birth_date']
    print(f"[RESULT] A11 entries: {a11_entries}")
    print(f"[RESULT] birth_date entries: {birth_date_entries}")
    
    # Run comprehensive test suite
    comprehensive_test_suite()
    
    # Test the new function parsing
    test_function_parsing()