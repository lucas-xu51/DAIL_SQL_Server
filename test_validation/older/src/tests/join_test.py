from collections import deque # For BFS queue used in connectivity check

class QueryValidator: # Encapsulating in a dummy class for self reference

    def __init__(self):
        # You would initialize schema info here if needed elsewhere
        pass

    def _validate_join(self, formal_sql, formal_schema):
        """
        Validates join conditions from formal_sql against formal_schema.

        Checks:
        1. Table Existence (after resolving aliases).
        2. Column Existence within the corresponding table.
        3. (Warning) If an equality join condition doesn't match a defined PK/FK relationship.
        4. Join Connectivity: Ensures all tables involved in joins are connected.

        Args:
            formal_sql: Dictionary representing the parsed SQL query. Expected keys:
                        'J' (set): Join conditions {(type, (tab1, col1), op, (tab2, col2)), ...}
                                    Table names in tuples can be aliases initially.
                        'alias' (set): Table aliases {(real_table, alias), ...}
            formal_schema: Dictionary representing the database schema. Expected keys:
                           'Tabs' (set): Set of table names.
                           'Cols' (dict): {table_name: {col1, col2, ...}, ...}
                           'PKs' (dict): {table_name: {pk_col1, ...}, ...}
                           'FKs' (list): [((src_tab, src_col), (tgt_tab, tgt_col)), ...]

        Returns:
            A list of error and warning strings. Empty list if all joins are valid.
        """
        # --- Setup ---
        join_conditions = formal_sql.get('J', set())
        aliases = formal_sql.get('alias', set())
        schema_tabs = formal_schema.get('Tabs', set())
        schema_cols = formal_schema.get('Cols', {})
        schema_fks = formal_schema.get('FKs', [])
        schema_pks = formal_schema.get('PKs', {})

        errors = []
        warnings = []
        resolved_joins = set() # Store joins with actual table names for connectivity check
        basic_validation_passed_for_all = True # Track if all joins pass basic checks

        # Create the alias map {alias: real_name} for quick lookup
        alias_to_real = {alias: real for real, alias in aliases}

        # Pre-build a set of FK pairs for faster relationship lookups
        # Store both forward and backward directions for easier matching
        fk_pairs = set()
        for (src_tab, src_col), (tgt_tab, tgt_col) in schema_fks:
            # Ensure FK definition itself uses valid tables/cols (optional sanity check)
            if src_tab in schema_tabs and tgt_tab in schema_tabs and \
               src_col in schema_cols.get(src_tab, set()) and \
               tgt_col in schema_cols.get(tgt_tab, set()):
                 fk_pairs.add(((src_tab, src_col), (tgt_tab, tgt_col)))
                 fk_pairs.add(((tgt_tab, tgt_col), (src_tab, src_col))) # Add reverse

        # --- 1. Basic Validation Loop (Table/Column checks) & Build Resolved Joins ---
        print("Join Conditions:")
        print(join_conditions)
        for join_condition in join_conditions:
            try:
                # Unpack the join tuple
                join_type, left_condition, operator, right_condition = join_condition
                left_table_alias, left_column = left_condition
                right_table_alias, right_column = right_condition
            except (ValueError, TypeError):
                errors.append(f"JOIN Error: Malformed join condition tuple skipped: {join_condition}")
                basic_validation_passed_for_all = False # Mark failure
                continue # Skip this malformed condition

            # Resolve Aliases to get actual table names
            # If the name is in the alias map, use the real name; otherwise, assume it's already the real name.
            left_table = alias_to_real.get(left_table_alias, left_table_alias)
            right_table = alias_to_real.get(right_table_alias, right_table_alias)

            current_join_valid = True # Flag for this specific join condition

            # --- Validation 1: Table Existence ---
            if left_table not in schema_tabs:
                errors.append(f"JOIN Error: Table '{left_table}' (from alias/name '{left_table_alias}') in JOIN condition does not exist in schema.")
                current_join_valid = False
            if right_table not in schema_tabs:
                errors.append(f"JOIN Error: Table '{right_table}' (from alias/name '{right_table_alias}') in JOIN condition does not exist in schema.")
                current_join_valid = False

            if not current_join_valid:
                basic_validation_passed_for_all = False # Mark failure
                continue # Cannot check columns if tables don't exist

            # --- Validation 2: Column Existence ---
            # Use .get() on schema_cols to avoid KeyError if table somehow passed tab check but not in cols
            if left_column not in schema_cols.get(left_table, set()):
                errors.append(f"JOIN Error: Column '{left_column}' does not exist in table '{left_table}' (alias/name '{left_table_alias}').")
                current_join_valid = False
            if right_column not in schema_cols.get(right_table, set()):
                errors.append(f"JOIN Error: Column '{right_column}' does not exist in table '{right_table}' (alias/name '{right_table_alias}').")
                current_join_valid = False

            if not current_join_valid:
                basic_validation_passed_for_all = False # Mark failure
                continue # Don't check relationships or add to resolved if columns invalid

            # --- If basic checks passed for this join, add resolved tuple and check relationship ---
            resolved_join_tuple = (join_type, (left_table, left_column), operator, (right_table, right_column))
            resolved_joins.add(resolved_join_tuple)

            # --- Validation 3: Relationship Check (Optional Warning for '=' joins) ---
            if operator == '=':
                is_fk_match = False
                # Check if the resolved join pair matches any defined FK (in either direction)
                join_pair_forward = ((left_table, left_column), (right_table, right_column))
                join_pair_backward = ((right_table, right_column), (left_table, left_column))
                if join_pair_forward in fk_pairs or join_pair_backward in fk_pairs:
                    is_fk_match = True

                # Check if joining two PKs (less common, but possible)
                is_pk_pk_match = (left_column in schema_pks.get(left_table, set()) and
                                  right_column in schema_pks.get(right_table, set()))

                # Add warning if this '=' join doesn't match a defined FK or a PK-PK link
                if not is_fk_match and not is_pk_pk_match:
                     warnings.append(f"JOIN Warning: Join condition '{left_table}.{left_column} = {right_table}.{right_column}' does not match a defined Foreign Key or a PK-PK relationship.")


        # --- 2. Connectivity Check (Only if basic validation passed for ALL joins and there are joins) ---
        if basic_validation_passed_for_all and resolved_joins:
            joined_tables = set()
            adj_list = {}

            # Build joined_tables set and adjacency list from resolved & validated joins
            for _, (t1, _), _, (t2, _) in resolved_joins:
                # These tables and columns are confirmed to exist from the loop above
                joined_tables.add(t1)
                joined_tables.add(t2)
                adj_list.setdefault(t1, set()).add(t2)
                adj_list.setdefault(t2, set()).add(t1) # Undirected edge

            # Perform graph traversal (BFS) only if there's more than one table involved
            if len(joined_tables) > 1:
                start_node = next(iter(joined_tables)) # Pick arbitrary start node
                visited = {start_node}
                queue = deque([start_node])

                while queue:
                    current_node = queue.popleft()
                    # Iterate over neighbors in the adjacency list
                    for neighbor in adj_list.get(current_node, set()):
                        # Check if neighbor is part of the intended join set and not visited
                        # (neighbor should always be in joined_tables if adj_list is built correctly)
                        if neighbor not in visited:
                            visited.add(neighbor)
                            queue.append(neighbor)

                # Compare visited set with all tables involved in joins
                if visited != joined_tables:
                    unvisited = joined_tables - visited
                    errors.append(f"JOIN Connectivity Error: The join conditions do not connect all involved tables. Disconnected tables: {unvisited}. Check for missing join conditions.")

        # --- Return collected errors and warnings ---
        return errors + warnings

# --- Example Usage (using the same schema and SQL examples as before) ---

# Dummy formal_schema based on your example (structure matters most)
formal_schema_example = {
    'Tabs': {'employees', 'departments', 'assignments', 'projects'},
    'Cols': {
        'employees': {'emp_id', 'emp_name', 'dept_id'},
        'departments': {'dept_id', 'dept_name'},
        'assignments': {'assign_id', 'emp_id', 'project_id', 'role', 'assign_date'},
        'projects': {'project_id', 'project_name', 'end_date'}
    },
    'PKs': {
        'employees': {'emp_id'},
        'departments': {'dept_id'},
        'assignments': {'assign_id'},
        'projects': {'project_id'}
    },
    'FKs': [
        (('employees', 'dept_id'), ('departments', 'dept_id')),
        (('assignments', 'emp_id'), ('employees', 'emp_id')),
        (('assignments', 'project_id'), ('projects', 'project_id')),
    ],
    'Types': {} # Assuming types are not needed for this validation
}

# Example formal_sql with joins containing aliases (as per your example)
formal_sql_example = {
    'J': {
        ('INNER', ('d', 'dept_id'), '=', ('e', 'dept_id')), # Aliases used
        ('INNER', ('a', 'emp_id'), '=', ('e', 'emp_id')),   # Aliases used
        ('INNER', ('a', 'project_id'), '=', ('p', 'project_id')) # Aliases used
     },
    'alias': {
        ('employees', 'e'),
        ('departments', 'd'),
        ('assignments', 'a'),
        ('projects', 'p')
    }
    # Other keys like T, C, F etc. would be here
}

# Example with a disconnected join
formal_sql_disconnected = {
    'J': {
        ('INNER', ('d', 'dept_id'), '=', ('e', 'dept_id')), # Connects d and e
        # Missing link between (d,e) and p
        ('INNER', ('p', 'project_id'), '=', ('p', 'project_id')) # Self join on p doesn't connect it
     },
    'alias': {
        ('employees', 'e'),
        ('departments', 'd'),
        ('assignments', 'a'), # 'a' is aliased but not used in J here
        ('projects', 'p')
    }
}

# Example with column error
formal_sql_col_error = {
     'J': {
        ('INNER', ('d', 'dept_id'), '=', ('e', 'department_id')), # e.department_id is wrong
        ('INNER', ('a', 'emp_id'), '=', ('e', 'emp_id')),
        ('INNER', ('a', 'project_id'), '=', ('p', 'project_id'))
     },
    'alias': {
        ('employees', 'e'), ('departments', 'd'), ('assignments', 'a'), ('projects', 'p')
    }
}

# --- Test Execution ---
validator = QueryValidator()

print("--- Test Case 1: Valid Joins with Aliases ---")
results1 = validator._validate_join(formal_sql_example, formal_schema_example)
print("Passed!" if not results1 else f"Issues:\n- " + "\n- ".join(results1))

print("\n--- Test Case 2: Disconnected Joins ---")
results2 = validator._validate_join(formal_sql_disconnected, formal_schema_example)
print("Passed!" if not results2 else f"Issues:\n- " + "\n- ".join(results2))
# Expected: Connectivity Error

print("\n--- Test Case 3: Column Error ---")
results3 = validator._validate_join(formal_sql_col_error, formal_schema_example)
print("Passed!" if not results3 else f"Issues:\n- " + "\n- ".join(results3))
# Expected: Column Error, Connectivity check likely skipped

print("\n--- Test Case 4: Empty Joins ---")
