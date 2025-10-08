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
            var (startDate, endDate) = GetPreviousMonthRange(DateTime.Today);
            var consumptions = await LoadConsumptionAsync(startDate, endDate);
            var fuelData = await LoadFuelDataAsync(startDate, endDate);

            if (consumptions.Count == 0)
            {
                Console.WriteLine("Nessun dato di consumo trovato per il mese precedente.");
                return;
            }

            Console.WriteLine($"Intervallo analizzato: {startDate:yyyy-MM-dd} - {endDate.AddDays(-1):yyyy-MM-dd}");
            var outputPath = Path.Combine(AppContext.BaseDirectory, "ReportConsumi.xlsx");
            GenerateReport(consumptions, fuelData, outputPath);
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
                     KG
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

            if (!reader.IsDBNull(ordinalLitri))
            {
                var liters = Convert.ToDouble(reader.GetValue(ordinalLitri), CultureInfo.InvariantCulture);
                aggregate.AddLiters(product, liters);
            }

            if (!reader.IsDBNull(ordinalKg))
            {
                var kg = Convert.ToDouble(reader.GetValue(ordinalKg), CultureInfo.InvariantCulture);
                aggregate.AddKg(product, kg);
            }
        }

        return result;
    }

    private static void GenerateReport(
        Dictionary<string, VehicleConsumptionAggregate> consumptions,
        Dictionary<string, FuelAggregate> fuelData,
        string outputPath)
    {
        using var workbook = new XLWorkbook();
        var metanoSheet = workbook.AddWorksheet("Metano");
        var dieselSheet = workbook.AddWorksheet("Diesel");
        var benzinaSheet = workbook.AddWorksheet("Benzina");

        WriteSheetHeader(metanoSheet, "Litri Benzina (BE)", "Kg totali riforniti (Risorse)");
        WriteSheetHeader(dieselSheet, "Litri AdBlue (AD)", "Litri totali riforniti (Risorse)");
        WriteSheetHeader(benzinaSheet, "Litri Extra", "Litri totali riforniti (Risorse)");
        FormatNumberColumns(metanoSheet);
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
            FuelAggregate? aggregateFuel = null;
            if (!string.IsNullOrEmpty(vehicle.NumeroInterno) && fuelData.TryGetValue(vehicle.NumeroInterno, out var fuelByNumero))
            {
                aggregateFuel = fuelByNumero;
            }
            else if (!string.IsNullOrEmpty(vehicle.Targa) && fuelData.TryGetValue(vehicle.Targa, out var fuelByTarga))
            {
                aggregateFuel = fuelByTarga;
            }

            if (aggregateFuel is null)
            {
                continue;
            }

            var hasMetano = aggregateFuel.HasAnyProduct(MetanoProducts);
            var hasDiesel = aggregateFuel.HasAnyProduct(DieselProducts);
            var hasBenzina = aggregateFuel.HasProduct(BenzinaProduct);

            var averageConsumption = vehicle.AverageConsumption;
            var totalKmForAverage = vehicle.TotalKm;
            var totalKm = vehicle.HasKmData ? (double?)vehicle.TotalKm : null;
            var totalConsumptionLiters = vehicle.HasConsumptionLiters ? (double?)vehicle.TotalConsumptionLiters : null;

            if (hasMetano)
            {
                var totalKg = aggregateFuel.GetTotalKg(MetanoProducts);
                double? metanoAverage = totalKmForAverage > 0 && totalKg > 0 ? (double?)(totalKmForAverage / totalKg) : null;
                var otherFuelLiters = aggregateFuel.GetTotalLitersExcluding(MetanoProducts);
                var totalKgValue = hasMetano ? (double?)totalKg : null;
                var otherFuelValue = otherFuelLiters > 0 ? (double?)otherFuelLiters : null;
                WriteRow(
                    metanoSheet,
                    currentRowMetano++,
                    vehicle.DisplayName,
                    averageConsumption,
                    metanoAverage,
                    otherFuelValue,
                    totalKm,
                    totalConsumptionLiters,
                    totalKgValue);
                continue;
            }

            if (hasDiesel)
            {
                var dieselLiters = aggregateFuel.GetTotalLiters(DieselProducts);
                double? dieselAverage = totalKmForAverage > 0 && dieselLiters > 0 ? (double?)(totalKmForAverage / dieselLiters) : null;
                var adBlueLiters = aggregateFuel.GetTotalLiters(AdBlueProduct);
                var dieselTotal = hasDiesel ? (double?)dieselLiters : null;
                var adBlueValue = adBlueLiters > 0 ? (double?)adBlueLiters : null;
                WriteRow(
                    dieselSheet,
                    currentRowDiesel++,
                    vehicle.DisplayName,
                    averageConsumption,
                    dieselAverage,
                    adBlueValue,
                    totalKm,
                    totalConsumptionLiters,
                    dieselTotal);
                continue;
            }

            if (hasBenzina)
            {
                var benzinaLiters = aggregateFuel.GetTotalLiters(BenzinaProduct);
                double? benzinaAverage = totalKmForAverage > 0 && benzinaLiters > 0 ? (double?)(totalKmForAverage / benzinaLiters) : null;
                var benzinaTotal = hasBenzina ? (double?)benzinaLiters : null;
                WriteRow(
                    benzinaSheet,
                    currentRowBenzina++,
                    vehicle.DisplayName,
                    averageConsumption,
                    benzinaAverage,
                    null,
                    totalKm,
                    totalConsumptionLiters,
                    benzinaTotal);
            }
        }

        metanoSheet.Columns().AdjustToContents();
        dieselSheet.Columns().AdjustToContents();
        benzinaSheet.Columns().AdjustToContents();

        workbook.SaveAs(outputPath);
    }

    private static void WriteSheetHeader(IXLWorksheet sheet, string extraColumnTitle, string replenishmentTotalTitle)
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
        double? replenishmentTotal)
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

        // dentro FuelAggregate
        internal double GetTotalKg(IEnumerable<string> products)
        {
            double total = 0;
            foreach (var p in products)
                if (_kgByProduct.TryGetValue(p, out var v)) total += v;
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
