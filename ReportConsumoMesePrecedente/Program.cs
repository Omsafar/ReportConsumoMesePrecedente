using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ClosedXML.Excel;
using Microsoft.Data.SqlClient;

namespace ReportConsumoMesePrecedente;

internal static class Program
{
    private const string ConnString =
        "Server=srv2016app02\\sgam;Database=PARATORI;User Id=sapara;Password=S@p4ra;Encrypt=True;TrustServerCertificate=True;";

    private const string ConnStringSgam =
        "Server=srv2016app02\\sgam;Database=SGAM;User Id=sapara;Password=S@p4ra;Encrypt=True;TrustServerCertificate=True;";

    private static readonly HashSet<string> DieselProducts = new(StringComparer.OrdinalIgnoreCase) { "HVO", "GA", "DP" };
    private static readonly HashSet<string> MetanoProducts = new(StringComparer.OrdinalIgnoreCase) { "GN", "GNL" };
    private const string BenzinaProduct = "BE";
    private const string AdBlueProduct = "AD";

    private static async Task Main()
    {
        try
        {
            var today = DateTime.Today;
            var (startDate, endDate) = GetPreviousMonthRange(today);
            var consumptions = await LoadConsumptionAsync(startDate, endDate);
            var fuelData = await LoadFuelDataAsync(startDate, endDate);
            var vehicleFuelTypes = await LoadVehicleFuelTypesAsync(today);

            if (consumptions.Count == 0)
            {
                Console.WriteLine("Nessun dato di consumo trovato per il mese precedente.");
                return;
            }

            Console.WriteLine($"Intervallo analizzato: {startDate:yyyy-MM-dd} - {endDate.AddDays(-1):yyyy-MM-dd}");
            var outputPath = Path.Combine(AppContext.BaseDirectory, "ReportConsumi.xlsx");
            GenerateReport(consumptions, fuelData, vehicleFuelTypes, outputPath);
            Console.WriteLine($"Report generato correttamente: {outputPath}");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Errore durante la generazione del report dei consumi:");
            Console.Error.WriteLine(ex);
        }
    }

    private static (DateTime Start, DateTime End) GetPreviousMonthRange(DateTime referenceDate)
    {
        var startOfCurrentMonth = new DateTime(referenceDate.Year, referenceDate.Month, 1);
        var startOfPreviousMonth = startOfCurrentMonth.AddMonths(-1);
        return (startOfPreviousMonth, startOfCurrentMonth);
    }

    private static async Task<Dictionary<string, VehicleConsumptionAggregate>> LoadConsumptionAsync(DateTime startDate, DateTime endDate)
    {
        var result = new Dictionary<string, VehicleConsumptionAggregate>(StringComparer.OrdinalIgnoreCase);

        await using var connection = new SqlConnection(ConnString);
        await connection.OpenAsync();

        const string query =
            @"SELECT Targa,
                     Numero_Interno,
                     Data,
                     Km_Totali,
                     [Consumo_km/l] AS ConsumoKmPerLitro,
                     [Litri_Totali] AS Litri
              FROM [PARATORI].[dbo].[tbDatiConsumo]
              WHERE Data >= @startDate AND Data < @endDate";

        await using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@startDate", SqlDbType.DateTime2) { Value = startDate });
        command.Parameters.Add(new SqlParameter("@endDate", SqlDbType.DateTime2) { Value = endDate });

        await using var reader = await command.ExecuteReaderAsync();
        var ordinalNumeroInterno = reader.GetOrdinal("Numero_Interno");
        var ordinalTarga = reader.GetOrdinal("Targa");
        var ordinalKmTotali = reader.GetOrdinal("Km_Totali");
        var ordinalConsumo = reader.GetOrdinal("ConsumoKmPerLitro");
        var ordinalLitri = SafeGetOrdinal(reader, "Litri");

        while (await reader.ReadAsync())
        {
            var numeroInternoRaw = reader.IsDBNull(ordinalNumeroInterno) ? null : reader.GetValue(ordinalNumeroInterno)?.ToString();
            var targaRaw = reader.IsDBNull(ordinalTarga) ? null : reader.GetValue(ordinalTarga)?.ToString();
            var numeroInterno = TrimToNull(numeroInternoRaw);
            var targa = TrimToNull(targaRaw);

            if (string.IsNullOrEmpty(numeroInterno) && string.IsNullOrEmpty(targa))
            {
                continue;
            }

            var key = numeroInterno ?? targa!;
            if (!result.TryGetValue(key, out var aggregate))
            {
                aggregate = new VehicleConsumptionAggregate(numeroInterno, targa);
                result[key] = aggregate;
            }
            else
            {
                aggregate.RegisterIdentifiers(numeroInterno, targa);
            }

            if (!reader.IsDBNull(ordinalKmTotali))
            {
                var km = Convert.ToDouble(reader.GetValue(ordinalKmTotali), CultureInfo.InvariantCulture);
                aggregate.AddKilometers(km);
            }

            if (!reader.IsDBNull(ordinalConsumo))
            {
                var consumo = Convert.ToDouble(reader.GetValue(ordinalConsumo), CultureInfo.InvariantCulture);
                if (!double.IsNaN(consumo) && !double.IsInfinity(consumo) && consumo > 0)
                {
                    aggregate.ConsumptionSum += consumo;
                    aggregate.ConsumptionCount++;
                }
            }

            if (ordinalLitri >= 0 && !reader.IsDBNull(ordinalLitri))
            {
                var liters = Convert.ToDouble(reader.GetValue(ordinalLitri), CultureInfo.InvariantCulture);
                aggregate.AddConsumptionLiters(liters);
            }
        }

        return result;
    }

    private static async Task<Dictionary<string, FuelAggregate>> LoadFuelDataAsync(DateTime startDate, DateTime endDate)
    {
        var result = new Dictionary<string, FuelAggregate>(StringComparer.OrdinalIgnoreCase);

        await using var connection = new SqlConnection(ConnStringSgam);
        await connection.OpenAsync();

        const string query =
            @"SELECT VEICOLO,
                     PRODOTTO,
                     LITRI,
                     KG,
                     CONTROVALORE_UNITARIO
              FROM [Sgam].[dbo].[RisorseRifornimentiRilevazioni]
              WHERE DATA_ORA >= @startDate AND DATA_ORA < @endDate";

        await using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@startDate", SqlDbType.DateTime2) { Value = startDate });
        command.Parameters.Add(new SqlParameter("@endDate", SqlDbType.DateTime2) { Value = endDate });

        await using var reader = await command.ExecuteReaderAsync();
        var ordinalVeicolo = reader.GetOrdinal("VEICOLO");
        var ordinalProdotto = reader.GetOrdinal("PRODOTTO");
        var ordinalLitri = reader.GetOrdinal("LITRI");
        var ordinalKg = reader.GetOrdinal("KG");
        var ordinalControvaloreUnitario = reader.GetOrdinal("CONTROVALORE_UNITARIO");

        while (await reader.ReadAsync())
        {
            var vehicleRaw = reader.IsDBNull(ordinalVeicolo) ? null : reader.GetValue(ordinalVeicolo)?.ToString();
            var vehicle = TrimToNull(vehicleRaw);
            if (string.IsNullOrEmpty(vehicle))
            {
                continue;
            }

            var productRaw = reader.IsDBNull(ordinalProdotto) ? null : reader.GetValue(ordinalProdotto)?.ToString();
            var product = TrimToNull(productRaw)?.ToUpperInvariant();
            if (string.IsNullOrEmpty(product))
            {
                continue;
            }

            if (!result.TryGetValue(vehicle, out var aggregate))
            {
                aggregate = new FuelAggregate();
                result[vehicle] = aggregate;
            }

            aggregate.RegisterProduct(product);

            double? liters = null;
            if (!reader.IsDBNull(ordinalLitri))
            {
                liters = Convert.ToDouble(reader.GetValue(ordinalLitri), CultureInfo.InvariantCulture);
                aggregate.AddLiters(product, liters.Value);
            }

            double? kg = null;
            if (!reader.IsDBNull(ordinalKg))
            {
                kg = Convert.ToDouble(reader.GetValue(ordinalKg), CultureInfo.InvariantCulture);
                aggregate.AddKg(product, kg.Value);
            }

            if (!reader.IsDBNull(ordinalControvaloreUnitario))
            {
                var unitValue = Convert.ToDouble(reader.GetValue(ordinalControvaloreUnitario), CultureInfo.InvariantCulture);
                var quantityForCost = GetQuantityForCost(product, liters, kg);
                if (quantityForCost.HasValue && quantityForCost.Value > 0 && unitValue > 0)
                {
                    aggregate.AddCost(product, quantityForCost.Value * unitValue);
                }
            }
        }

        return result;
    }

    private static async Task<Dictionary<string, string>> LoadVehicleFuelTypesAsync(DateTime date)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        await using var connection = new SqlConnection(ConnString);
        await connection.OpenAsync();

        await using var command = new SqlCommand("Stp_AnagraficaMezziUnica", connection)
        {
            CommandType = CommandType.StoredProcedure
        };

        command.Parameters.Add(new SqlParameter("@Tipo", SqlDbType.Int) { Value = 7 });
        command.Parameters.Add(new SqlParameter("@MostraTabella", SqlDbType.Int) { Value = 1 });
        command.Parameters.Add(new SqlParameter("@Alienati", SqlDbType.Char, 1) { Value = "N" });
        command.Parameters.Add(new SqlParameter("@Data", SqlDbType.VarChar, 8)
        {
            Value = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture)
        });

        await using var reader = await command.ExecuteReaderAsync();
        var ordinalTarga = SafeGetOrdinal(reader, "Targa");
        if (ordinalTarga < 0)
        {
            ordinalTarga = SafeGetOrdinal(reader, "TARGA");
        }

        var ordinalFuelType = SafeGetOrdinal(reader, "TIPO_CARABURANTE");
        if (ordinalFuelType < 0)
        {
            ordinalFuelType = SafeGetOrdinal(reader, "TIPO_CARBURANTE");
        }

        if (ordinalTarga < 0 || ordinalFuelType < 0)
        {
            throw new InvalidOperationException(
                "La stored procedure Stp_AnagraficaMezziUnica non restituisce le colonne attese Targa e TIPO_CARABURANTE.");
        }

        while (await reader.ReadAsync())
        {
            var targaRaw = reader.IsDBNull(ordinalTarga) ? null : reader.GetValue(ordinalTarga)?.ToString();
            var fuelTypeRaw = reader.IsDBNull(ordinalFuelType) ? null : reader.GetValue(ordinalFuelType)?.ToString();

            var targa = TrimToNull(targaRaw);
            var fuelType = TrimToNull(fuelTypeRaw);

            if (string.IsNullOrEmpty(targa) || string.IsNullOrEmpty(fuelType))
            {
                continue;
            }

            result[targa] = fuelType.ToUpperInvariant();
        }

        return result;
    }

    private static void GenerateReport(
        Dictionary<string, VehicleConsumptionAggregate> consumptions,
        Dictionary<string, FuelAggregate> fuelData,
        Dictionary<string, string> vehicleFuelTypes,
        string outputPath)
    {
        using var workbook = new XLWorkbook();
        var metanoSheet = workbook.AddWorksheet("Metano");
        var dieselSheet = workbook.AddWorksheet("Diesel");
        var benzinaSheet = workbook.AddWorksheet("Benzina");

        const string costColumnTitle = "Costo totale rifornimenti (Risorse)";
        WriteSheetHeader(metanoSheet, "Litri Benzina (BE)", "Kg totali riforniti (Risorse)", costColumnTitle);
        WriteSheetHeader(dieselSheet, "Litri AdBlue (AD)", "Litri totali riforniti (Risorse)", costColumnTitle);
        WriteSheetHeader(benzinaSheet, "Litri Extra", "Litri totali riforniti (Risorse)", costColumnTitle); FormatNumberColumns(metanoSheet);
        FormatNumberColumns(dieselSheet);
        FormatNumberColumns(benzinaSheet);

        var orderedVehicles = consumptions
            .Select(pair => pair.Value)
            .OrderBy(v => v.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var currentRowMetano = 2;
        var currentRowDiesel = 2;
        var currentRowBenzina = 2;

        foreach (var vehicle in orderedVehicles)
        {
            var fuelType = GetFuelTypeForVehicle(vehicle, vehicleFuelTypes);
            if (string.IsNullOrEmpty(fuelType))
            {
                continue;
            }

            FuelAggregate? aggregateFuel = null;
            if (!string.IsNullOrEmpty(vehicle.NumeroInterno) && fuelData.TryGetValue(vehicle.NumeroInterno, out var fuelByNumero))
            {
                aggregateFuel = fuelByNumero;
            }
            else if (!string.IsNullOrEmpty(vehicle.Targa) && fuelData.TryGetValue(vehicle.Targa, out var fuelByTarga))
            {
                aggregateFuel = fuelByTarga;
            }

            var averageConsumption = vehicle.AverageConsumption;
            var totalKmForAverage = vehicle.TotalKm;
            var totalKm = vehicle.HasKmData ? (double?)vehicle.TotalKm : null;
            var totalConsumptionLiters = vehicle.HasConsumptionLiters ? (double?)vehicle.TotalConsumptionLiters : null;

            if (string.Equals(fuelType, "ME", StringComparison.OrdinalIgnoreCase))
            {
                double? metanoAverage = null;
                double? otherFuelValue = null;
                double? totalKgValue = null;
                double? metanoCostValue = null;

                if (aggregateFuel is not null)
                {
                    var totalKg = aggregateFuel.GetTotalKg(MetanoProducts);
                    if (totalKmForAverage > 0 && totalKg > 0)
                    {
                        metanoAverage = totalKmForAverage / totalKg;
                    }

                    if (totalKg > 0)
                    {
                        totalKgValue = totalKg;
                    }

                    var otherFuelLiters = aggregateFuel.GetTotalLitersExcluding(MetanoProducts);
                    if (otherFuelLiters > 0)
                    {
                        otherFuelValue = otherFuelLiters;
                    }

                    var metanoCost = aggregateFuel.GetTotalCost(MetanoProducts);
                    if (metanoCost > 0)
                    {
                        metanoCostValue = metanoCost;
                    }
                }

                WriteRow(
                    metanoSheet,
                    currentRowMetano++,
                    vehicle.DisplayName,
                    averageConsumption,
                    metanoAverage,
                    otherFuelValue,
                    totalKm,
                    totalConsumptionLiters,
                    totalKgValue,
                    metanoCostValue);
                continue;
            }

            if (string.Equals(fuelType, "GA", StringComparison.OrdinalIgnoreCase))
            {
                double? dieselAverage = null;
                double? adBlueValue = null;
                double? dieselTotal = null;
                double? dieselCostValue = null;

                if (aggregateFuel is not null)
                {
                    var dieselLiters = aggregateFuel.GetTotalLiters(DieselProducts);
                    if (totalKmForAverage > 0 && dieselLiters > 0)
                    {
                        dieselAverage = totalKmForAverage / dieselLiters;
                    }

                    if (dieselLiters > 0)
                    {
                        dieselTotal = dieselLiters;
                    }

                    var adBlueLiters = aggregateFuel.GetTotalLiters(AdBlueProduct);
                    if (adBlueLiters > 0)
                    {
                        adBlueValue = adBlueLiters;
                    }

                    var dieselCost = aggregateFuel.GetTotalCost(DieselProducts);
                    if (dieselCost > 0)
                    {
                        dieselCostValue = dieselCost;
                    }
                }

                WriteRow(
                    dieselSheet,
                    currentRowDiesel++,
                    vehicle.DisplayName,
                    averageConsumption,
                    dieselAverage,
                    adBlueValue,
                    totalKm,
                    totalConsumptionLiters,
                    dieselTotal,
                    dieselCostValue);
                continue;
            }

            if (string.Equals(fuelType, "BE", StringComparison.OrdinalIgnoreCase))
            {
                double? benzinaAverage = null;
                double? benzinaTotal = null;
                double? benzinaCostValue = null;

                if (aggregateFuel is not null)
                {
                    var benzinaLiters = aggregateFuel.GetTotalLiters(BenzinaProduct);
                    if (totalKmForAverage > 0 && benzinaLiters > 0)
                    {
                        benzinaAverage = totalKmForAverage / benzinaLiters;
                    }

                    if (benzinaLiters > 0)
                    {
                        benzinaTotal = benzinaLiters;
                    }

                    var benzinaCost = aggregateFuel.GetTotalCost(BenzinaProduct);
                    if (benzinaCost > 0)
                    {
                        benzinaCostValue = benzinaCost;
                    }
                }

                WriteRow(
                    benzinaSheet,
                    currentRowBenzina++,
                    vehicle.DisplayName,
                    averageConsumption,
                    benzinaAverage,
                    null,
                    totalKm,
                    totalConsumptionLiters,
                    benzinaTotal,
                    benzinaCostValue);
            }
        }

        metanoSheet.Columns().AdjustToContents();
        dieselSheet.Columns().AdjustToContents();
        benzinaSheet.Columns().AdjustToContents();

        workbook.SaveAs(outputPath);
    }

    private static string? GetFuelTypeForVehicle(
        VehicleConsumptionAggregate vehicle,
        Dictionary<string, string> vehicleFuelTypes)
    {
        if (vehicle is null)
        {
            return null;
        }

        if (!string.IsNullOrEmpty(vehicle.Targa) && vehicleFuelTypes.TryGetValue(vehicle.Targa, out var fuelType))
        {
            return fuelType;
        }

        return null;
    }

    private static void WriteSheetHeader(
        IXLWorksheet sheet,
        string extraColumnTitle,
        string replenishmentTotalTitle,
        string costColumnTitle)
    {
        sheet.Cell(1, 1).Value = "Veicolo";
        sheet.Cell(1, 2).Value = "Media km/l (DatiConsumo)";
        sheet.Cell(1, 3).Value = "Consumo medio rifornimenti (km/l)";
        if (!string.IsNullOrEmpty(extraColumnTitle))
        {
            sheet.Cell(1, 4).Value = extraColumnTitle;
        }
        sheet.Cell(1, 5).Value = "Km totali (DatiConsumo)";
        sheet.Cell(1, 6).Value = "Litri totali (DatiConsumo)";
        if (!string.IsNullOrEmpty(replenishmentTotalTitle))
        {
            sheet.Cell(1, 7).Value = replenishmentTotalTitle;
        }
        sheet.Cell(1, 8).Value = costColumnTitle;
        sheet.Row(1).Style.Font.SetBold();
    }

    private static void FormatNumberColumns(IXLWorksheet sheet)
    {
        sheet.Column(2).Style.NumberFormat.SetFormat("0.00");
        sheet.Column(3).Style.NumberFormat.SetFormat("0.000");
        sheet.Column(4).Style.NumberFormat.SetFormat("0.00");
        sheet.Column(5).Style.NumberFormat.SetFormat("0.00");
        sheet.Column(6).Style.NumberFormat.SetFormat("0.00");
        sheet.Column(7).Style.NumberFormat.SetFormat("0.00");
        sheet.Column(8).Style.NumberFormat.SetFormat("0.00");
    }

    private static double? GetQuantityForCost(string product, double? liters, double? kg)
    {
        if (string.IsNullOrEmpty(product))
        {
            return liters ?? kg;
        }

        if (MetanoProducts.Contains(product))
        {
            return kg ?? liters;
        }

        if (DieselProducts.Contains(product) || string.Equals(product, BenzinaProduct, StringComparison.OrdinalIgnoreCase))
        {
            return liters ?? kg;
        }

        if (string.Equals(product, AdBlueProduct, StringComparison.OrdinalIgnoreCase))
        {
            return liters ?? kg;
        }

        return liters ?? kg;
    }

    private static void WriteRow(
        IXLWorksheet sheet,
        int row,
        string veicolo,
        double? averageConsumption,
        double? fuelAverage,
        double? extraValue,
        double? totalKm,
        double? totalConsumptionLiters,
        double? replenishmentTotal,
        double? totalCost)
    {
        sheet.Cell(row, 1).Value = veicolo;
        if (averageConsumption.HasValue)
        {
            sheet.Cell(row, 2).Value = averageConsumption.Value;
        }

        if (fuelAverage.HasValue)
        {
            sheet.Cell(row, 3).Value = fuelAverage.Value;
        }

        if (extraValue.HasValue)
        {
            sheet.Cell(row, 4).Value = extraValue.Value;
        }

        if (totalKm.HasValue)
        {
            sheet.Cell(row, 5).Value = totalKm.Value;
        }

        if (totalConsumptionLiters.HasValue)
        {
            sheet.Cell(row, 6).Value = totalConsumptionLiters.Value;
        }

        if (replenishmentTotal.HasValue)
        {
            sheet.Cell(row, 7).Value = replenishmentTotal.Value;
        }


        if (totalCost.HasValue)
        {
            sheet.Cell(row, 8).Value = totalCost.Value;
        }
    }

    private static string? TrimToNull(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return value.Trim();
    }

    private sealed class VehicleConsumptionAggregate
    {
        internal VehicleConsumptionAggregate(string? numeroInterno, string? targa)
        {
            NumeroInterno = numeroInterno;
            Targa = targa;
        }

        internal string? NumeroInterno { get; private set; }
        internal string? Targa { get; private set; }
        internal double TotalKm { get; private set; }
        internal bool HasKmData { get; private set; }
        internal double TotalConsumptionLiters { get; private set; }
        internal bool HasConsumptionLiters { get; private set; }
        internal double ConsumptionSum { get; set; }
        internal int ConsumptionCount { get; set; }

        internal double? AverageConsumption => ConsumptionCount > 0 ? ConsumptionSum / ConsumptionCount : null;

        internal string DisplayName
        {
            get
            {
                if (!string.IsNullOrEmpty(Targa))
                {
                    if (!string.IsNullOrEmpty(NumeroInterno) && !string.Equals(Targa, NumeroInterno, StringComparison.OrdinalIgnoreCase))
                    {
                        return $"{Targa} ({NumeroInterno})";
                    }

                    return Targa;
                }

                return NumeroInterno ?? string.Empty;
            }
        }

        internal void RegisterIdentifiers(string? numeroInterno, string? targa)
        {
            if (!string.IsNullOrEmpty(numeroInterno))
            {
                NumeroInterno ??= numeroInterno;
            }

            if (!string.IsNullOrEmpty(targa))
            {
                Targa ??= targa;
            }
        }

        internal void AddKilometers(double kilometers)
        {
            TotalKm += kilometers;
            HasKmData = true;
        }

        internal void AddConsumptionLiters(double liters)
        {
            TotalConsumptionLiters += liters;
            HasConsumptionLiters = true;
        }
    }

    private sealed class FuelAggregate
    {
        private readonly Dictionary<string, double> _litersByProduct = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, double> _kgByProduct = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _products = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, double> _costByProduct = new(StringComparer.OrdinalIgnoreCase);

        internal void RegisterProduct(string product)
        {
            _products.Add(product);
        }

        internal bool HasProduct(string product) => _products.Contains(product);

        internal bool HasAnyProduct(IEnumerable<string> products)
        {
            foreach (var product in products)
            {
                if (_products.Contains(product))
                {
                    return true;
                }
            }

            return false;
        }

        internal void AddLiters(string product, double liters)
        {
            if (_litersByProduct.TryGetValue(product, out var existing))
            {
                _litersByProduct[product] = existing + liters;
            }
            else
            {
                _litersByProduct[product] = liters;
            }
        }

        internal void AddKg(string product, double kg)
        {
            if (_kgByProduct.TryGetValue(product, out var existing))
            {
                _kgByProduct[product] = existing + kg;
            }
            else
            {
                _kgByProduct[product] = kg;
            }
        }

        internal void AddCost(string product, double cost)
        {
            if (_costByProduct.TryGetValue(product, out var existing))
            {
                _costByProduct[product] = existing + cost;
            }
            else
            {
                _costByProduct[product] = cost;
            }
        }

        internal double GetTotalLiters(string product)
        {
            return _litersByProduct.TryGetValue(product, out var value) ? value : 0;
        }

        internal double GetTotalLiters(IEnumerable<string> products)
        {
            double total = 0;
            foreach (var product in products)
            {
                if (_litersByProduct.TryGetValue(product, out var value))
                {
                    total += value;
                }
            }

            return total;
        }

        internal double GetTotalLitersExcluding(IEnumerable<string> excludedProducts)
        {
            var exclusions = new HashSet<string>(excludedProducts, StringComparer.OrdinalIgnoreCase);
            double total = 0;
            foreach (var pair in _litersByProduct)
            {
                if (!exclusions.Contains(pair.Key))
                {
                    total += pair.Value;
                }
            }

            return total;
        }

        internal double GetTotalKg(IEnumerable<string> products)
        {
            double total = 0;
            foreach (var product in products)
            {
                if (_kgByProduct.TryGetValue(product, out var value))
                {
                    total += value;
                }
            }

            return total;
        }

        internal double GetTotalCost(string product)
        {
            return _costByProduct.TryGetValue(product, out var value) ? value : 0;
        }

        internal double GetTotalCost(IEnumerable<string> products)
        {
            double total = 0;
            foreach (var product in products)
            {
                if (_costByProduct.TryGetValue(product, out var value))
                {
                    total += value;
                }
            }

            return total;
        }
    }

    private static int SafeGetOrdinal(SqlDataReader reader, string columnName)
    {
        try
        {
            return reader.GetOrdinal(columnName);
        }
        catch (IndexOutOfRangeException)
        {
            return -1;
        }
    }
}
