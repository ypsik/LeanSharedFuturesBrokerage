using CryptoExchange.Net.Web3;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.Common
{
    /// <summary>
    /// EthKey validates the 0x prefix; configs may store addresses/keys without it.
    /// </summary>
    internal static class EthKeyHelper
    {
        private static string With0x(string value)
        {
            value = value.Trim();
            return value.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? value : "0x" + value;
        }

        public static EthKey FromPublicAddress(string address) => EthKey.FromPublicAddress(With0x(address));

        public static EthKey FromPrivateKey(string privateKey) => EthKey.FromPrivateKey(With0x(privateKey));
    }
}
