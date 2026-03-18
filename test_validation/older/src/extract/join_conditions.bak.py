import sqlparse
from sqlparse.sql import Identifier, Comparison, Where, Function, TokenList, Comment
from sqlparse.tokens import Keyword, DML, Punctuation, Operator, Comment as CommentToken

# --- Paste the previously corrected functions _resolve_identifier, _canonicalize_join, extract_join_conditions here ---
def _resolve_identifier(token, alias_map):
    """
    Attempts to resolve a token (Identifier, Function) into (table_name, column_name_or_expression).
    Resolves aliases using alias_map.
    Returns (resolved_table, column_name_or_repr) or (None, None) if resolution fails.
    """
    if isinstance(token, Identifier):
        parent_name = token.get_parent_name()
        real_name = token.get_real_name() # This is the column name

        resolved_table = None
        # Check if the identifier itself is an alias (e.g., SELECT t1 FROM table1 t1) - less common in columns
        # More likely, the parent is the alias/table
        if parent_name:
            resolved_table = alias_map.get(parent_name, parent_name)
        # If no parent, table is None (ambiguous or context-dependent)
        return resolved_table, real_name

    elif isinstance(token, Function):
        # Basic attempt: return the whole function call as the 'column'
        # Try to find a table if an identifier inside has one
        for sub_token in token.get_parameters():
             if isinstance(sub_token, Identifier):
                 parent_name = sub_token.get_parent_name()
                 if parent_name:
                      resolved_table = alias_map.get(parent_name, parent_name)
                      # Return resolved table and the string representation of the function
                      return resolved_table, str(token)
        # If no identifier with a parent is found within the function params
        return None, str(token)

    # Handle cases where token might be a direct reference or complex structure
    elif isinstance(token, TokenList): # Handle cases like Parenthesis, Function parameters, etc.
        # Attempt to simplify if it's a list containing just one significant item (Identifier or Function)
        # Useful for cases like GROUP BY (col1) or SELECT (t1.col2)
        significant_tokens = []
        # Check if token has 'tokens' attribute (Parenthesis, Function etc. do)
        if hasattr(token, 'tokens'):
            for sub_token in token.tokens:
                 # Ignore whitespace, comments, and the parentheses/commas themselves if inside a list
                 if sub_token.is_whitespace or isinstance(sub_token, Comment) or \
                    sub_token.match(Punctuation, '(') or sub_token.match(Punctuation, ')') or \
                    sub_token.match(Punctuation, ','):
                     continue
                 significant_tokens.append(sub_token)

        # If we found exactly one significant token inside, try resolving that token instead
        if len(significant_tokens) == 1:
            # Recursive call to resolve the inner token
            inner_token = significant_tokens[0]
            # Use the correct function name here (_resolve_sql_identifier_helper or _resolve_identifier)
            return _resolve_identifier(inner_token, alias_map)
        else:
            # Otherwise, return the string representation of the whole TokenList
            # This handles complex expressions like (col1 + col2) or lists like function parameters
            return None, str(token)


    # If it's not an identifier or function, likely a literal or operator - cannot be resolved to table/col
    return None, None


def _canonicalize_join(join_tuple):
    """Sorts table/column pairs in a join tuple for consistent representation
       if the operator is symmetric (like '=')."""
    join_type, (t1, c1), op, (t2, c2) = join_tuple

    # Define symmetric operators
    symmetric_ops = {'=', '<>', '!='} # Add others like LIKE if treated symmetrically

    # Ensure we have valid table names and column names for comparison
    if t1 is None or t2 is None or c1 is None or c2 is None:
         return join_tuple # Cannot canonicalize if parts are ambiguous

    if op in symmetric_ops:
        # Compare based on table name first, then column name
        if (t1, c1) > (t2, c2):
             # Swap sides
             # Find the inverse operator if necessary (though '=' is its own inverse)
             # For now, assume operator remains the same for symmetric ones
             return (join_type, (t2, c2), op, (t1, c1))

    # Return original or potentially swapped tuple
    return (join_type, (t1, c1), op, (t2, c2))


def extract_join_conditions(parsed, aliases):
    """
    Extracts JOIN conditions (explicit and implicit) from a sqlparse result.

    Args:
        parsed: sqlparse.parse() result (the Statement object).
        aliases: A set of tuples {(real_table, alias), ...}

    Returns:
        A set of join condition tuples:
        {(join_type, (table1, column1), operator, (table2, column2)), ...}
    """
    join_conditions = set()
    alias_map = {alias: real for real, alias in aliases}
    rev_alias_map = {real: alias for alias, real in alias_map.items()} # Map real name back to alias

    # --- Process Explicit Joins (JOIN clauses) ---
    from_or_join_seen = False
    current_join_type = 'IMPLICIT' # Default
    last_table_identified = None # Track the table just before the JOIN/ON
    active_join_table = None # Table introduced by the current JOIN
    active_join_type = None # Type of the current JOIN

    tokens = parsed.tokens # Use structured tokens

    i = 0
    while i < len(tokens):
        token = tokens[i]
        # Handle potential None token if parsing is weird, though unlikely
        if token is None:
            i += 1
            continue

        token_value_upper = token.value.upper()

        # Skip whitespace and comments - FIXED HERE
        if token.is_whitespace or isinstance(token, Comment) or token.ttype in (CommentToken.Single, CommentToken.Multiline):
            i += 1
            continue

        # Find FROM
        if not from_or_join_seen and token.ttype is Keyword and token_value_upper == 'FROM':
            from_or_join_seen = True
            # Find the first table identifier after FROM
            j = i + 1
            processed_from_table = False
            while j < len(tokens):
                sub_token = tokens[j]
                if sub_token is None: j+=1; continue # Safety check
                if sub_token.is_whitespace or isinstance(sub_token, Comment):
                    j += 1
                    continue
                if isinstance(sub_token, Identifier):
                     # An identifier can be a table name optionally followed by an alias
                     real_name = sub_token.get_real_name()
                     alias_name = sub_token.get_alias()

                     # Decide which name represents the table in this context
                     table_name_for_context = real_name # Usually the real name is what we track

                     # Add to alias map if not already present (e.g. implicit alias from FROM table alias)
                     if alias_name and (real_name, alias_name) not in aliases:
                           # This case might not be needed if aliases are pre-extracted reliably
                           alias_map[alias_name] = real_name
                           rev_alias_map[real_name] = alias_name
                           aliases.add((real_name, alias_name)) # Keep master list consistent

                     last_table_identified = table_name_for_context

                     processed_from_table = True
                     i = j # Continue processing after this identifier token
                     break

                elif isinstance(sub_token, TokenList): # Handle cases like FROM (SELECT ...) alias
                     sub_alias = sub_token.get_alias()
                     if sub_alias:
                         # Use alias name, mark as subquery for clarity
                         # We don't have a 'real name' for the subquery result easily
                         last_table_identified = sub_alias + " (subquery)" # Mark as subquery
                         # Add this alias mapping if needed elsewhere? Map subquery alias to itself?
                         alias_map[sub_alias] = sub_alias + " (subquery)"
                         rev_alias_map[sub_alias + " (subquery)"] = sub_alias
                         aliases.add((sub_alias + " (subquery)", sub_alias))

                         processed_from_table = True
                         # Find the index of the alias token to continue after it
                         alias_token_idx = -1
                         for k_idx, t in enumerate(sub_token.tokens):
                             if isinstance(t, Identifier) and t.value == sub_alias:
                                 # This assumes alias is *part* of the TokenList, which might not be right
                                 # Often alias is *after* the list. Let's check that instead.
                                 pass # Need to check token *after* sub_token (TokenList)

                         # Check token after the TokenList for alias
                         next_real_token_idx = j + 1
                         while next_real_token_idx < len(tokens) and tokens[next_real_token_idx].is_whitespace:
                              next_real_token_idx += 1
                         if next_real_token_idx < len(tokens) and isinstance(tokens[next_real_token_idx], Identifier) and tokens[next_real_token_idx].value == sub_alias:
                              i = next_real_token_idx # Continue after alias
                         else: # Alias wasn't immediately after, assume part of list or syntax error
                              i = j # Continue after TokenList

                         break
                     else: # No alias found, cannot identify this subquery easily
                        i = j # Stop searching after the TokenList
                        break

                elif sub_token.ttype is Keyword: # Hit next clause before finding table?
                     i = j - 1 # Re-evaluate the keyword
                     break
                j += 1
            if not processed_from_table: # If loop finished without finding table
                i = j - 1
            i += 1
            continue # Process next token after FROM clause element


        # Find JOIN clauses if we are after FROM
        if from_or_join_seen and token.ttype is Keyword:
            is_join_keyword = False
            # Determine join type
            if token_value_upper == 'JOIN':
                active_join_type = 'INNER'
                is_join_keyword = True
            elif token_value_upper in ('LEFT', 'RIGHT', 'FULL', 'CROSS'):
                active_join_type = token_value_upper
                # Skip 'OUTER' if present
                peek_idx = i + 1
                while peek_idx < len(tokens) and (tokens[peek_idx] is None or tokens[peek_idx].is_whitespace or isinstance(tokens[peek_idx], Comment)): peek_idx += 1
                if peek_idx < len(tokens) and tokens[peek_idx].value.upper() == 'OUTER':
                    i = peek_idx # Consume OUTER
                # Skip 'JOIN' if present after type (e.g., LEFT JOIN)
                peek_idx = i + 1
                while peek_idx < len(tokens) and (tokens[peek_idx] is None or tokens[peek_idx].is_whitespace or isinstance(tokens[peek_idx], Comment)): peek_idx += 1
                if peek_idx < len(tokens) and tokens[peek_idx].value.upper() == 'JOIN':
                    i = peek_idx # Consume JOIN
                is_join_keyword = True
            elif token_value_upper == 'INNER' or token_value_upper == 'INNER JOIN':
                active_join_type = 'INNER'
                peek_idx = i + 1
                while peek_idx < len(tokens) and (tokens[peek_idx] is None or tokens[peek_idx].is_whitespace or isinstance(tokens[peek_idx], Comment)): peek_idx += 1
                if peek_idx < len(tokens) and tokens[peek_idx].value.upper() == 'JOIN':
                    i = peek_idx # Consume JOIN
                is_join_keyword = True

            if is_join_keyword:
                # Find the table being joined TO (should be next Identifier or subquery)
                j = i + 1
                found_join_table = False
                while j < len(tokens):
                    sub_token = tokens[j]
                    if sub_token is None: j+=1; continue
                    if sub_token.is_whitespace or isinstance(sub_token, Comment):
                        j += 1
                        continue
                    if isinstance(sub_token, Identifier):
                        # Get real table name and alias
                        real_name = sub_token.get_real_name()
                        alias_name = sub_token.get_alias()

                        active_join_table = real_name # Track the real name primarily

                        # Update alias maps if needed
                        if alias_name and (real_name, alias_name) not in aliases:
                            alias_map[alias_name] = real_name
                            rev_alias_map[real_name] = alias_name
                            aliases.add((real_name, alias_name))

                        i = j # Continue processing after the joined table identifier
                        found_join_table = True
                        break

                    elif isinstance(sub_token, TokenList): # Handle JOIN (SELECT ...) alias
                         sub_alias = sub_token.get_alias()
                         if sub_alias:
                             active_join_table = sub_alias + " (subquery)"
                             alias_map[sub_alias] = sub_alias + " (subquery)"
                             rev_alias_map[sub_alias + " (subquery)"] = sub_alias
                             aliases.add((sub_alias + " (subquery)", sub_alias))
                             found_join_table = True
                             # Find index of alias token after list
                             next_real_token_idx = j + 1
                             while next_real_token_idx < len(tokens) and tokens[next_real_token_idx].is_whitespace:
                                  next_real_token_idx += 1
                             if next_real_token_idx < len(tokens) and isinstance(tokens[next_real_token_idx], Identifier) and tokens[next_real_token_idx].value == sub_alias:
                                  i = next_real_token_idx # Continue after alias
                             else:
                                  i = j # Continue after TokenList
                             break
                         else: # No alias found
                            i = j # Stop after TokenList
                            break
                    else:
                         # Expected identifier or subquery after JOIN keyword(s)
                         active_join_table = None # Reset if not found
                         i = j - 1 # Re-process the non-identifier token
                         break
                    j+=1
                if not found_join_table:
                    i = j - 1


                # Now look for the ON clause immediately after finding the join table
                k = i + 1
                on_clause_tokens = []
                on_found = False
                processed_on = False
                while k < len(tokens):
                     sub_token = tokens[k]
                     if sub_token is None: k+=1; continue
                     if sub_token.is_whitespace or isinstance(sub_token, Comment):
                          k+=1
                          continue
                     if sub_token.ttype is Keyword and sub_token.value.upper() == 'ON':
                           on_found = True
                           # Capture tokens after ON until the next major clause
                           m = k + 1
                           while m < len(tokens):
                               next_token = tokens[m]
                               if next_token is None: m+=1; continue
                               # Check for end of ON condition (next JOIN, WHERE, GROUP etc.)
                               if next_token.ttype is Keyword and next_token.value.upper() in ('JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT'):
                                     break
                               # Add the token to our list for analysis
                               if not (next_token.is_whitespace or isinstance(next_token, Comment)):
                                    on_clause_tokens.append(next_token)
                               m += 1
                           i = m - 1 # Continue processing from the keyword that ended the ON clause
                           processed_on = True
                           break # Exit ON search loop
                     else:
                          # If we found JOIN table but not ON immediately after (ignoring whitespace)
                          # Treat as CROSS JOIN or maybe USING clause (not handled yet)
                          # Stop looking for ON for this JOIN.
                          i = k - 1 # Re-process this token
                          break
                     k += 1
                if not processed_on: # If loop finished without finding/processing ON
                    i = k - 1


                if on_found and active_join_table:
                    # Process the collected ON clause tokens
                    # Wrap in a temporary TokenList to use sqlparse's Comparison detection
                    on_condition = TokenList(on_clause_tokens)

                    def find_comparisons_in_on(token_list_obj):
                        local_joins = set()
                        # Check if token_list_obj has tokens attribute
                        if not hasattr(token_list_obj, 'tokens'):
                            return local_joins

                        for sub_token in token_list_obj.tokens: # Iterate through tokens in the list
                            if sub_token is None: continue
                            if isinstance(sub_token, Comparison):
                                left = sub_token.left
                                right = sub_token.right
                                # Find comparison operator more reliably
                                op_token = None
                                for t_idx, t in enumerate(sub_token.tokens):
                                     if t is None: continue
                                     if t.ttype in Operator.Comparison:
                                          op_token = t
                                          break

                                if op_token: # Check if comparison operator found
                                     op = op_token.value
                                     l_table, l_col = _resolve_identifier(left, alias_map)
                                     r_table, r_col = _resolve_identifier(right, alias_map)

                                     # Check: is it a valid join condition? Needs two columns.
                                     # Basic check: both sides resolved, columns exist
                                     if l_col and r_col:
                                         resolved_l_table = l_table
                                         resolved_r_table = r_table
                                         is_active_subquery = active_join_table and isinstance(active_join_table, str) and active_join_table.endswith("(subquery)")
                                         is_last_subquery = last_table_identified and isinstance(last_table_identified, str) and last_table_identified.endswith("(subquery)")

                                         # Heuristic for unqualified column: assume one is last table, other is active table
                                         # Only apply heuristic if context tables are actual tables (not None or subqueries)
                                         if active_join_table and not is_active_subquery and last_table_identified and not is_last_subquery :
                                             if l_table is None and r_table == active_join_table :
                                                 resolved_l_table = last_table_identified
                                             elif r_table is None and l_table == active_join_table :
                                                 resolved_r_table = last_table_identified
                                             # Heuristic if BOTH are None (less likely in ON, but possible)
                                             elif l_table is None and r_table is None:
                                                 # Cannot reliably determine, skip adding join? Or make risky guess?
                                                 # Let's skip for now to avoid wrong joins.
                                                 continue # Skip this comparison

                                         # Add if resolution worked and tables seem valid
                                         # Ensure resolved tables aren't None before final check
                                         if resolved_l_table and resolved_r_table:
                                              # Ensure tables aren't marked as subqueries before adding join condition
                                             is_resolved_l_subquery = isinstance(resolved_l_table, str) and resolved_l_table.endswith("(subquery)")
                                             is_resolved_r_subquery = isinstance(resolved_r_table, str) and resolved_r_table.endswith("(subquery)")

                                             if not is_resolved_l_subquery and not is_resolved_r_subquery:
                                                  join_tuple = (active_join_type, (resolved_l_table, l_col), op, (resolved_r_table, r_col))
                                                  local_joins.add(_canonicalize_join(join_tuple))


                            elif isinstance(sub_token, TokenList): # Recurse into brackets etc.
                                local_joins.update(find_comparisons_in_on(sub_token))
                        return local_joins

                    join_conditions.update(find_comparisons_in_on(on_condition))

                # Reset for next potential join clause
                # Only update last_table_identified if we successfully identified the active join table
                # Also check it's not a subquery placeholder
                if active_join_table and not (isinstance(active_join_table, str) and active_join_table.endswith("(subquery)")):
                    last_table_identified = active_join_table
                active_join_type = None
                active_join_table = None
                # i is already updated

        # Stop processing JOINs if we hit other major clauses
        elif from_or_join_seen and token.ttype is Keyword and token_value_upper in ('WHERE', 'GROUP', 'ORDER', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT'):
             # Need to stop join processing, let WHERE handle implicit joins
             from_or_join_seen = False # Turn off flag
             # No need to reset last_table_identified here, WHERE might need it implicitly
             # Let the main loop continue to find WHERE etc.

        i += 1


    # --- Process Implicit Joins (WHERE clause) ---
    where_clause = None
    for token in parsed.tokens: # Iterate through top-level tokens of the statement
        if token is None: continue
        if isinstance(token, Where):
            where_clause = token
            break

    if where_clause:
        # Need to handle context for ambiguous columns if possible
        # For simplicity, we'll rely on columns being qualified or aliases defined
        # Getting the full list of tables in scope is harder

        def find_comparisons_recursive(token_list_obj): # Accept TokenList/Where object
            local_joins = set()
            # Check if token_list has tokens attribute
            if not hasattr(token_list_obj, 'tokens'):
                 return local_joins

            for token in token_list_obj.tokens:
                 if token is None: continue
                 if isinstance(token, Comparison):
                     left = token.left
                     right = token.right
                     # Find the comparison operator token
                     op_token = None
                     for t_idx, t in enumerate(token.tokens):
                           if t is None: continue
                           if t.ttype in Operator.Comparison:
                                op_token = t
                                break

                     if op_token: # Ensure operator was found
                        op = op_token.value
                        l_table, l_col = _resolve_identifier(left, alias_map)
                        r_table, r_col = _resolve_identifier(right, alias_map)

                        # Check if it's an implicit join:
                        # 1. Both sides resolved to table/columns (not literals/None)
                        # 2. The tables are different
                        # 3. Tables are not marked as subqueries
                        is_l_subquery = isinstance(l_table, str) and l_table.endswith("(subquery)")
                        is_r_subquery = isinstance(r_table, str) and r_table.endswith("(subquery)")

                        if l_table and r_table and l_col and r_col and l_table != r_table and \
                           not is_l_subquery and not is_r_subquery:
                             join_tuple = ('IMPLICIT', (l_table, l_col), op, (r_table, r_col))
                             local_joins.add(_canonicalize_join(join_tuple))

                 # Recurse into sub-lists (like parenthesized conditions, AND/OR groups)
                 # Exclude Identifier to prevent infinite recursion if Identifier contains tokens (unlikely)
                 elif isinstance(token, TokenList) and not isinstance(token, Identifier):
                     local_joins.update(find_comparisons_recursive(token)) # Pass the TokenList itself
            return local_joins

        join_conditions.update(find_comparisons_recursive(where_clause)) # Start recursion with the Where object


    return join_conditions

# if __name__ == "__main__":
#     # --- Testing ---
#     # Test Case 1 (Explicit Join)
#     sql = 'SELECT e.emp_id, e.emp_name, d.dept_name, p.project_name, a.role, a.assign_date FROM employees e INNER JOIN departments d ON e.dept_id = d.  dept_id INNER JOIN assignments a ON e.emp_id = a.emp_id INNER JOIN projects p ON a.project_id = p.project_id WHERE p.end_date > CURRENT_DATE ORDER BY     d.dept_name, e.emp_name;'
#     parsed = sqlparse.parse(sql)[0]
#     aliases = {('card', 'T1'), ('foreign_data', 'T2')}
#     joins = extract_join_conditions(parsed, aliases.copy()) # Pass copy as function might modify it
#     print(f"Extracted Joins: {joins}")


#     print("-" * 20)

#     # Test Case 2 (Implicit Join)
#     sql2 = "SELECT t1.name, t3.city FROM table1 t1, table2 t2, table3 t3 WHERE t1.id = t2.ref_id AND t2.city_id = t3.id AND t1.age > 30"
#     parsed2 = sqlparse.parse(sql2)[0]
#     aliases2 = {('table1', 't1'), ('table2', 't2'), ('table3', 't3')}
#     joins2 = extract_join_conditions(parsed2, aliases2.copy())
#     print(f"SQL: {sql2}")
#     print(f"Extracted Joins: {joins2}")


#     print("-" * 20)

#     # Test Case 3 (Explicit LEFT JOIN)
#     sql3 = "SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE c.country = 'USA'"
#     parsed3 = sqlparse.parse(sql3)[0]
#     aliases3 = {('orders', 'o'), ('customers', 'c')}
#     joins3 = extract_join_conditions(parsed3, aliases3.copy())
#     print(f"SQL: {sql3}")
#     print(f"Extracted Joins: {joins3}")


#     print("-" * 20)

#     # Test Case 4 (Multiple Explicit Joins)
#     sql4 = "SELECT p.name, c.category_name, s.supplier_name FROM products p JOIN categories c ON p.category_id = c.id JOIN suppliers s ON p.supplier_id =   s.id WHERE c.category_name = 'Electronics';"
#     parsed4 = sqlparse.parse(sql4)[0]
#     aliases4 = {('products', 'p'), ('categories', 'c'), ('suppliers', 's')}
#     joins4 = extract_join_conditions(parsed4, aliases4.copy())
#     print(f"SQL: {sql4}")
#     print(f"Extracted Joins: {joins4}")


#     print("-" * 20)

#     # Test Case 5 (Join with comment)
#     sql5 = """
#     SELECT o.order_id, c.customer_name -- Get order and customer name
#     FROM orders AS o -- Use alias o for orders
#     INNER JOIN customers AS c -- Use alias c for customers
#       ON o.customer_id = c.customer_id -- Join on customer ID
#     WHERE o.order_date > '2023-01-01';
#     """
#     parsed5 = sqlparse.parse(sql5)[0]
#     aliases5 = {('orders', 'o'), ('customers', 'c')}
#     joins5 = extract_join_conditions(parsed5, aliases5.copy())
#     print(f"SQL: {sql5}")
#     print(f"Extracted Joins: {joins5}")