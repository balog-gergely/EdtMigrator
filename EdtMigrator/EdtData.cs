using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace EdtMigrator
{
    public class EdtData
    {
        public string EdtName { get; set; }
        public string NameOfParentEdt { get; set; }
        public List<Relation> Relations { get; set; }
        public EdtData(XmlDocument doc)
        {
            EdtName = doc.SelectSingleNode("//Name").InnerText;
            NameOfParentEdt = FindNameOfParent(doc); ;
            Relations = new List<Relation>();
        }
      private string FindNameOfParent(XmlDocument document)
        {
            var extendsNode = document.SelectSingleNode("//Extends");
            if (extendsNode != null)
                return extendsNode.InnerText;
            else
                return null;
        }
    }
}
