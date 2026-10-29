# UrbanMart Data Platform — End-to-End Medallion Lakehouse & Dimensional Modeling

## 1. Επισκόπηση Έργου & Αρχιτεκτονική (Architecture Overview)
Το παρόν project υλοποιεί μια ολοκληρωμένη πλατφόρμα δεδομένων (Data Lakehouse) στο περιβάλλον του **Databricks** (Unity Catalog: `bootcamp`, Schemas: `silver_marios_urban` και `gold_marios_urban`). Στόχος της πλατφόρμας είναι η διαχείριση, ο καθαρισμός, η διαστασιακή μοντελοποίηση κατά Kimball (Star Schema) και η παροχή αξιόπιστων επιχειρηματικών αναλύσεων και KPI dashboards για την αλυσίδα λιανικής **UrbanMart**[cite: 3, 4].

Η ροή των δεδομένων ακολουθεί αυστηρά την αρχιτεκτονική **Medallion (Bronze → Silver → Gold)**, ενορχηστρωμένη πλήρως μέσω **Databricks Workflows / Lakeflow Jobs**[cite: 3]:
* **Landing / Bronze:** Raw ingestion των 6 αρχείων CSV μέσω Volumes (`read_files()`), διατήρηση όλων των στηλών ως strings και προσθήκη τεχνικών πεδίων ιχνηλασιμότητας (`_source_file`, `_ingested_at`, `_source_row_hash`)[cite: 5].
* **Silver Layer:** Καθαρισμός, τυποποίηση τύπων δεδομένων (casting), υπολογισμός derived επιχειρηματικών στηλών και πλήρης καταγραφή των απορρίψεων στον πίνακα `etl_rejects`.
* **Gold Layer:** Σχεδιασμός Kimball Star Schema με Surrogate Keys, Conformed Dimensions, Fact table με Liquid Clustering και διαχείριση Unknown Members (`-1`).
* **Validation Gate:** Αυτοματοποιημένοι έλεγχοι με `assert_true()` που διασφαλίζουν την ποιότητα πριν την κατανάλωση δεδομένων.
* **Analytics & AI/BI:** 5 αναλυτικά queries αποκλειστικά από το Gold layer και δημοσιευμένο executive dashboard[cite: 3].

---

## 2. Profiling & Data Quality (DQ) Log
Κατά τη διερεύνηση των πρωτογενών δεδομένων (Bronze profiling), εντοπίστηκαν όλα τα εσκεμμένα σφάλματα ποιότητας (seeded defects). Αντί για σιωπηρή διαγραφή γραμμών, εφαρμόστηκε διαχωρισμός και καταγραφή στον πίνακα `silver_marios_urban.etl_rejects` (**138 απορρίψεις συνολικά**):

| Πίνακας Πηγής | Εντοπισμένο Πρόβλημα (Defect) | Πλήθος Γραμμών | Κανόνας & Τρόπος Διαχείρισης (Handling Rule) |
| :--- | :--- | :---: | :--- |
| **`brz_customers`** | Duplicate records (`customer_id`) | **8** | Αποδιπλασιασμός (deduplication) με χρήση `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY registration_date DESC)`. Διατηρήθηκε η πιο πρόσφατη εγγραφή στο `slv_customers` και οι 8 διπλότυπες απομονώθηκαν στο `etl_rejects`. |
| **`brz_customers`** | Missing / Empty Email | **15** | Αποκλείστηκαν από το Silver (`email IS NOT NULL AND trim(email) != ''`) και οδηγήθηκαν στο `etl_rejects` λόγω έλλειψης βασικού στοιχείου επικοινωνίας. |
| **`brz_order_items`** | Non-positive quantity (`quantity <= 0`) | **113** | Αποκλείστηκαν από το καθαρό dataset (`quantity > 0`) και καταγράφηκαν στο `etl_rejects`. |
| **`brz_order_items`** | Orphan line item (`L9999999`) | **1** | Παραβίαση αναφορικής ακεραιότητας: η παραγγελία `O999999` δεν υπήρχε στο `orders.csv`. Απομονώθηκε στο `etl_rejects`. |
| **`brz_products`** | Uncatalogued product (`P9999`) | **1** | Το προϊόν δεν υπήρχε στον επίσημο κατάλογο. Απομονώθηκε στο `etl_rejects` και ταυτόχρονα συνδέθηκε με το Unknown Member (`product_key = -1`) στο Gold layer. |
| **Σύνολο Απορρίψεων** | **Όλα τα εντοπισμένα defects** | **138** | **Πλήρες audit trail στο `silver.etl_rejects`** |

---

## 3. Διαστασιακός Σχεδιασμός κατά Kimball (Dimensional Design)

### 3.1. Business Process & Grain
* **Business Process:** Πωλήσεις λιανικής (Retail Sales Orders) της UrbanMart σε φυσικά καταστήματα και online[cite: 6, 7].
* **Fact Grain (Δήλωση σε 1 πρόταση):** *«One row in `fact_sales` represents an individual line item within a customer's sales order.»* (Μία γραμμή ανά μεμονωμένο προϊόν παραγγελίας)[cite: 6, 7].

### 3.2. Ταξινόμηση Μετρικών (Measure Classification)
* **Additive (Προσθετικές):** `quantity`, `gross_amount`, `discount_amount`, `net_amount`, `cost_amount`, `margin_amount` (αθροίζονται έγκυρα σε όλες τις διαστάσεις: χρόνο, κατάστημα, προϊόν, πελάτη).
* **Non-Additive (Μη-προσθετικές):** `unit_price`, `cost_price`, `discount_pct`, `margin_pct` (τιμές μονάδας και ποσοστά που δεν επιδέχονται απλό άθροισμα, αλλά υπολογίζονται ως σταθμισμένοι μέσοι όροι).
* **Semi-Additive (Ημι-προσθετικές):** Υπόλοιπα αποθεμάτων ανά κατάστημα/ημερομηνία.

### 3.3. Unknown Members (`Key = -1`)
Για την αποφυγή απώλειας οικονομικών συναλλαγών από το Fact table σε περιπτώσεις ελλιπούς πληροφορίας, εισήχθη σε κάθε διάσταση εγγραφή με Surrogate Key **`-1`**:
* `dim_customer`: Customer Key `-1` (*Unknown Customer*).
* `dim_product`: Product Key `-1` (*Unknown Product*).
* `dim_store`: Store Key `-1` (*Unknown Store*).
* `dim_date`: Date Key `-1` (*Unknown Date*).

### 3.4. Σχεδιασμός Star Schema
* **Dimensions:**
  * `dim_date`: 731 ημέρες (καλύπτει πλήρως τη διετία 2024–2025 με flags όπως `is_weekend`, μήνες, τρίμηνα).
  * `dim_customer`: 486 εγγραφές (485 καθαροί πελάτες + 1 Unknown Member).
  * `dim_product`: 76 εγγραφές (75 καθαρά προϊόντα + 1 Unknown Member).
  * `dim_store`: 14 εγγραφές (13 καταστήματα + 1 Unknown Member).
* **Fact Table:**
  * `fact_sales`: 23.997 εγγραφές με Surrogate Keys προς όλες τις διαστάσεις, τεχνικό primary key (`sales_key`) και επιχειρηματικά μέτρα.

---

## 4. Τεχνική Υλοποίηση & Σχεδιαστικές Επιλογές (Design Decisions & Trade-offs)

### 4.1. Γιατί επιλέχθηκαν τα συγκεκριμένα Joins
* **Στη δημιουργία του `fact_sales` (`LEFT JOIN` + `COALESCE(-1)`):**  
  Κατά τη συνένωση του Silver `slv_order_items` με τις διαστάσεις, χρησιμοποιήθηκε αποκλειστικά `LEFT JOIN` σε συνδυασμό με `COALESCE(dim.key, -1)`. Με αυτή την επιλογή, εάν υπάρξει κάποιο άγνωστο ή ελαττωματικό κλειδί (όπως το προϊόν `P9999`), η συναλλαγή δεν απορρίπτεται αλλά εντάσσεται κανονικά στον πίνακα πωλήσεων συνδεδεμένη με τον άγνωστο κόμβο (`-1`). Έτσι, δεν χάνεται ούτε ένα ευρώ από τον συνολικό τζίρο της εταιρείας.
* **Στα Analytics Queries (Q1–Q5) (`INNER JOIN` με φίλτρα):**  
  Στα τελικά αναλυτικά ερωτήματα χρησιμοποιήθηκαν `INNER JOIN` με ρητό αποκλεισμό των άγνωστων μελών (`WHERE f.product_key != -1` και `WHERE f.customer_key != -1`), ώστε οι αναλύσεις συμπεριφοράς πελατών, κερδοφορίας brands και επιστροφών να αντικατοπτρίζουν αυστηρά τις πραγματικές και γνωστές επιχειρηματικές οντότητες[cite: 2].

### 4.2. Liquid Clustering έναντι Static Partitioning
Στον πίνακα `fact_sales` επιλέχθηκε **Liquid Clustering** (`CLUSTER BY order_date_key`) αντί για κλασικό Hive partitioning (`PARTITIONED BY year, month`).  
* **Αιτιολόγηση:** Το Liquid Clustering αποτρέπει το φαινόμενο υπερβολικού κατακερματισμού σε μικρά αρχεία (small files problem), προσαρμόζεται δυναμικά καθώς ο όγκος δεδομένων αυξάνεται και παρέχει άριστο data skipping στα χρονικά φίλτρα των αναφορών.

### 4.3. Επαναληψιμότητα (Idempotency) & Constraints
* Όλα τα βήματα υλοποιήθηκαν με λογική `CREATE OR REPLACE TABLE` (ή `MERGE INTO`), εξασφαλίζοντας ότι το pipeline μπορεί να επανεκτελεστεί οποιαδήποτε στιγμή χωρίς να δημιουργούνται διπλότυπες εγγραφές ή ασυνέπειες.
* Στο `fact_sales` ορίστηκαν table constraints: `CHECK (net_amount >= 0)` και `CHECK (quantity > 0)`.

---

## 5. Δυσκολίες που Αντιμετωπίστηκαν & Επιλύσεις (Challenges & Solutions)

1. **Διαχείριση Ορφανής Γραμμής Παραγγελίας:**
   * *Πρόβλημα:* Η γραμμή `L9999999` στο `order_items.csv` αναφερόταν στην παραγγελία `O999999`, η οποία δεν υπήρχε στο `orders.csv`.
   * *Λύση:* Κατά το Silver build έγινε διασταύρωση με `LEFT JOIN` προς το `slv_orders`. Όπου το `order_id` βρέθηκε `NULL`, η εγγραφή απομονώθηκε αυτόματα στο `etl_rejects` με σαφή αιτιολογία referential integrity violation.
2. **Διαχείριση Μη Καταλογογραφημένου Προϊόντος (`P9999`):**
   * *Πρόβλημα:* Υπήρχε πώληση προϊόντος που δεν υπήρχε στον κατάλογο προϊόντων.
   * *Λύση:* Αντί να διαγραφεί η πώληση (που θα μείωνε πλασματικά τα έσοδα), συνδέθηκε με το `product_key = -1` (*Unknown Product*). Έτσι, ο τζίρος καταγράφηκε στο Fact Table, αλλά εξαιρέθηκε από τις αναλύσεις κατηγοριών.
3. **Αποτροπή Division by Zero:**
   * *Πρόβλημα:* Κατά τον υπολογισμό ποσοστών κέρδους (`margin_pct`) και μέσης αξίας παραγγελίας (`avg_order_value`), υπήρχε κίνδυνος σφάλματος διαίρεσης με το μηδέν σε περιπτώσεις μηδενικών εσόδων ή παραγγελιών.
   * *Λύση:* Εφαρμόστηκε συστηματικά η συνάρτηση `NULLIF(παρονομαστής, 0)` σε όλα τα aggregation queries.
4. **Αυτοματοποιημένο Quality Gate:**
   * *Πρόβλημα:* Ανάγκη διασφάλισης ότι τυχόν σφάλμα στα δεδομένα δεν θα περάσει απαρατήρητο στο Gold layer.
   * *Λύση:* Ενσωματώθηκε notebook ελέγχων (`04_validation`) με εντολές `assert_true()`, το οποίο λειτουργεί ως τελικό gate και διακόπτει άμεσα το workflow σε περίπτωση αποτυχίας[cite: 1].

---

## 6. Επαλήθευση & Ορθότητα Αποτελεσμάτων (Data Accuracy Proof)

Όλα τα νούμερα και τα αποτελέσματα του κώδικα επαληθεύτηκαν μαθηματικά μέσω cross-query διασταυρώσεων:

1. **Reconciliation Πλήθους Γραμμών:**
   * Αρχικές γραμμές `brz_order_items`: **24.111**
   * Απορρίψεις στο `etl_rejects`: **114** (113 με `quantity <= 0` + 1 ορφανή `L9999999`)
   * Καθαρές γραμμές `slv_order_items` και `fact_sales`: **23.997** ($24.111 - 114 = 23.997$).
2. **Reconciliation Εσόδων & Κέρδους (Cross-Check Q2 vs Q5):**
   * **Συνολικό Revenue (Q5):**  
     Weekday (€7.170.446,66) + Weekend (€2.873.743,57) = **€10.044.190,23**[cite: 2].
   * **Συνολικό Margin (Q5):**  
     Weekday (€2.112.742,37) + Weekend (€849.483,48) = **€2.962.225,85**[cite: 2].
   * Το άθροισμα των 25 γραμμών του **Q2** (Brand & Category breakdown) ταυτίζεται επακριβώς με τα παραπάνω σύνολα[cite: 2].
3. **Reconciliation Παραγγελιών & Τεμαχίων:**
   * Σύνολο παραγγελιών: **8.000** (ταυτίζεται απόλυτα μεταξύ `slv_orders` και `fact_sales`).
   * Σύνολο τεμαχίων: 42.426 (Weekday) + 17.410 (Weekend) = **59.836 units** (ταυτίζεται με το άθροισμα ποσοτήτων του Silver)[cite: 2].
4. **Ανάλυση Επιστροφών (Q4):**
   * Σύνολο γραμμών που αναλύθηκαν: **23.996** (23.997 μείον 1 για το `product_key = -1`)[cite: 2].
   * Τα ποσοστά επιστροφών (`return_rate_pct`) υπολογίστηκαν με απόλυτη ακρίβεια επί των επιμέρους γραμμών[cite: 2].

---

## 7. Ενορχήστρωση (Orchestration), Delta Features & AI/BI Dashboard

1. **Lakeflow Job (Databricks Workflows):**
   * Δημιουργήθηκε το DAG job **`UrbanMart_Job_MariosKatsanevakis`** που εκτελεί διαδοχικά[cite: 7]:
     $$\mathbf{setup} \longrightarrow \mathbf{bronze} \longrightarrow \mathbf{silver} \longrightarrow \mathbf{gold} \longrightarrow \mathbf{validation}$$
   * Το job εκτελέστηκε επιτυχώς σε **3 λεπτά και 25 δευτερόλεπτα** (`Succeeded`), με το `validation` να λειτουργεί ως το τελικό επισφράγισμα ποιότητας[cite: 7].
2. **Delta History & Time Travel:**
   * Εκτελέστηκε `DESCRIBE HISTORY bootcamp.silver_marios_urban.slv_customers` αποτυπώνοντας πλήρως τις εκδόσεις (versions 0, 1, 2), τα αντίστοιχα operations και timestamps[cite: 8].
   * Εκτελέστηκε Time Travel query (`VERSION AS OF 0`), ανακτώντας με επιτυχία το snapshot των 485 εγγραφών[cite: 8].
3. **AI/BI Executive Dashboard:**
   * Δημοσιεύτηκε διαδραστικό dashboard με 4 βασικά KPI counters (Total Revenue: €10.063.191, Total Margin: €2.963.316, Total Units: 59.826, Orders: 7.994) και 5 δυναμικά visuals (Monthly Revenue Trend, Category Breakdown, Store Breakdown, Customer Geographic Distribution, Product Performance Table)[cite: 2, 4].